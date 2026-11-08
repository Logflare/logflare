defmodule Logflare.Backends.Adaptor.VictoriaMetricsAdaptorTest do
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.WebhookAdaptor.Pipeline
  alias Logflare.Backends.AdaptorSupervisor
  alias Logflare.SystemMetrics.AllLogsLogged

  @subject Logflare.Backends.Adaptor.VictoriaMetricsAdaptor
  @client Logflare.Backends.Adaptor.WebhookAdaptor.Client

  # docker-compose `vm` service — see docker-compose.yml
  @vm_remote_write_url "http://localhost:8428/api/v1/write"

  setup do
    start_supervised!(AllLogsLogged)
    :ok
  end

  describe "cast_config/validate_config" do
    setup do
      stub(Logflare.Utils.SSRF, :safe_resolve, fn _ -> {:ok, {1, 2, 3, 4}} end)
      :ok
    end

    test "url is required" do
      refute Adaptor.cast_and_validate_config(@subject, %{}).valid?
    end

    test "valid with url only" do
      assert Adaptor.cast_and_validate_config(@subject, %{"url" => @vm_remote_write_url}).valid?
    end

    test "rejects invalid url" do
      refute Adaptor.cast_and_validate_config(@subject, %{"url" => "not-a-url"}).valid?
    end

    test "requires both username and password when either provided" do
      refute Adaptor.cast_and_validate_config(@subject, %{
               "url" => "http://vm:8428/api/v1/write",
               "username" => "user"
             }).valid?

      refute Adaptor.cast_and_validate_config(@subject, %{
               "url" => "http://vm:8428/api/v1/write",
               "password" => "pass"
             }).valid?

      assert Adaptor.cast_and_validate_config(@subject, %{
               "url" => "http://vm:8428/api/v1/write",
               "username" => "user",
               "password" => "pass"
             }).valid?
    end

    test "accepts optional labels map" do
      assert Adaptor.cast_and_validate_config(@subject, %{
               "url" => "http://vm:8428/api/v1/write",
               "labels" => %{"env" => "prod"}
             }).valid?
    end

    test "accepts an optional query base URL and allows clearing it" do
      existing = %{
        url: @vm_remote_write_url,
        query_url: "https://vmselect.example.com/select/0/prometheus"
      }

      assert Adaptor.cast_and_validate_config(@subject, existing).valid?

      for query_url <- [nil, ""] do
        changeset = Adaptor.cast_and_validate_config(@subject, %{query_url: query_url}, existing)
        assert changeset.valid?
        assert Ecto.Changeset.get_field(changeset, :query_url) == nil
      end
    end

    test "rejects query base URLs with invalid transport or embedded request options" do
      for query_url <- [
            "not-a-url",
            "ftp://vm.example.com",
            "https:///api",
            "https://vm.example.com:0",
            "https://vm.example.com:65536",
            "https://user:password@vm.example.com",
            "https://vm.example.com?query=up",
            "https://vm.example.com#fragment"
          ] do
        changeset =
          Adaptor.cast_and_validate_config(@subject, %{
            url: @vm_remote_write_url,
            query_url: query_url
          })

        refute changeset.valid?, query_url
        assert changeset.errors[:query_url]
      end
    end

    test "returns a validation error when editing the query URL to a malformed port" do
      existing = %{
        url: "https://8.8.8.8/api/v1/write",
        query_url: "https://8.8.8.8"
      }

      changeset =
        Adaptor.cast_and_validate_config(
          @subject,
          %{query_url: "https://8.8.8.8:notaport"},
          existing
        )

      refute changeset.valid?
      assert changeset.errors[:query_url]
    end

    test "requires password reentry for a new read origin while keeping write-only headers" do
      for previous_query_url <- [nil, "https://reader.example.com/select/0/prometheus"] do
        existing = %{
          url: "https://vm.example.com/api/v1/write",
          query_url: previous_query_url,
          username: "user",
          password: "pass",
          headers: %{"authorization" => "Bearer write-token"}
        }

        params =
          existing
          |> @subject.redact_config()
          |> Map.put(:query_url, "https://other-reader.example.com/select/0/prometheus")

        changeset = Adaptor.cast_and_validate_config(@subject, params, existing)
        refute changeset.valid?
        assert changeset.errors[:password]
        assert Ecto.Changeset.get_field(changeset, :password) == nil
        assert Ecto.Changeset.get_field(changeset, :headers) == existing.headers

        reentered =
          Adaptor.cast_and_validate_config(@subject, Map.put(params, :password, "pass"), existing)

        assert reentered.valid?
        assert Ecto.Changeset.get_field(reentered, :password) == "pass"
      end
    end

    test "keeps shared credentials for same-origin read paths and when disabling reads" do
      existing = %{
        url: "https://vm.example.com/api/v1/write",
        username: "user",
        password: "pass"
      }

      for old_url <- [nil, "https://vm.example.com/select/0/prometheus"],
          new_url <- [nil, "https://vm.example.com/select/1/prometheus"] do
        stored = Map.put(existing, :query_url, old_url)
        params = stored |> @subject.redact_config() |> Map.put(:query_url, new_url)
        changeset = Adaptor.cast_and_validate_config(@subject, params, stored)

        assert changeset.valid?
        assert Ecto.Changeset.get_field(changeset, :password) == "pass"
        assert Ecto.Changeset.get_field(changeset, :query_url) == new_url
      end
    end

    test "requires credential reentry when both write and read origins change" do
      existing = %{
        url: "https://vm.example.com/api/v1/write",
        query_url: "https://reader.example.com/select/0/prometheus",
        username: "user",
        password: "pass",
        headers: %{"authorization" => "Bearer token", "x-tenant" => "acme"}
      }

      params =
        existing
        |> @subject.redact_config()
        |> Map.merge(%{
          url: "https://new-vm.example.com/api/v1/write",
          query_url: "https://new-reader.example.com/select/0/prometheus"
        })

      changeset = Adaptor.cast_and_validate_config(@subject, params, existing)
      refute changeset.valid?
      assert changeset.errors[:password]
      assert Ecto.Changeset.get_field(changeset, :password) == nil
      assert Ecto.Changeset.get_field(changeset, :headers) == %{"x-tenant" => "acme"}

      reentered =
        params
        |> Map.merge(%{password: existing.password, headers: existing.headers})
        |> then(&Adaptor.cast_and_validate_config(@subject, &1, existing))

      assert reentered.valid?
      assert Ecto.Changeset.get_field(reentered, :password) == existing.password
      assert Ecto.Changeset.get_field(reentered, :headers) == existing.headers
      assert Ecto.Changeset.get_field(reentered, :url) == params.url
      assert Ecto.Changeset.get_field(reentered, :query_url) == params.query_url
    end

    test "treats query URL scheme and port changes as a new destination" do
      existing = %{
        url: "https://vm.example.com/api/v1/write",
        query_url: "https://reader.example.com/select/0/prometheus",
        username: "user",
        password: "pass"
      }

      for query_url <- ["http://reader.example.com", "https://reader.example.com:8443"] do
        changeset = Adaptor.cast_and_validate_config(@subject, %{query_url: query_url}, existing)
        refute changeset.valid?
        assert changeset.errors[:password]
      end
    end

    test "keeps stored credentials submitted back in redacted form" do
      existing = %{
        url: "https://user:secret@vm.example.com/api/v1/write",
        username: "user",
        password: "pass",
        headers: %{"authorization" => "Bearer token", "x-tenant" => "acme"}
      }

      params =
        existing
        |> @subject.redact_config()
        |> Map.put(:headers, %{"Authorization" => "REDACTED", "x-tenant" => "acme"})

      changeset = Adaptor.cast_and_validate_config(@subject, params, existing)

      assert changeset.valid?
      config = Ecto.Changeset.apply_changes(changeset)
      assert config.url == existing.url
      assert config.password == "pass"
      assert config.headers == %{"authorization" => "Bearer token", "x-tenant" => "acme"}
    end

    test "does not carry stored credentials to a different host" do
      existing = %{
        url: "https://vm.example.com/api/v1/write",
        username: "user",
        password: "pass",
        headers: %{"authorization" => "Bearer token", "x-tenant" => "acme"}
      }

      redacted = @subject.redact_config(existing)

      moved =
        Adaptor.cast_and_validate_config(
          @subject,
          %{redacted | url: "https://other.example.com/api/v1/write"},
          existing
        )

      refute moved.valid?
      assert {_msg, _opts} = moved.errors[:password]
      config = Ecto.Changeset.apply_changes(moved)
      assert config.password == nil
      assert config.headers == %{"x-tenant" => "acme"}

      url_only =
        Adaptor.cast_and_validate_config(
          @subject,
          %{url: "https://other.example.com/api/v1/write"},
          existing
        )

      config = Ecto.Changeset.apply_changes(url_only)
      assert config.password == nil
      assert config.headers == %{"x-tenant" => "acme"}

      reentered =
        Adaptor.cast_and_validate_config(
          @subject,
          %{redacted | url: "https://other.example.com/api/v1/write", password: "new"},
          existing
        )

      assert reentered.valid?
      assert Ecto.Changeset.apply_changes(reentered).password == "new"
    end

    test "keeps explicitly reentered identical credentials when moving the write destination" do
      existing = %{
        url: "https://vm.example.com/api/v1/write",
        headers: %{"authorization" => "Bearer token", "x-tenant" => "acme"}
      }

      changeset =
        Adaptor.cast_and_validate_config(
          @subject,
          %{existing | url: "https://other.example.com/api/v1/write"},
          existing
        )

      assert changeset.valid?
      assert Ecto.Changeset.get_field(changeset, :headers) == existing.headers
    end

    test "drops masked credentials in mixed header edits only when the write origin changes" do
      existing = %{
        url: "https://vm.example.com/api/v1/write",
        headers: %{"authorization" => "Bearer token", "x-tenant" => "acme"}
      }

      for {url, expected_headers} <- [
            {"https://other.example.com/api/v1/write", %{"x-tenant" => "updated"}},
            {"https://vm.example.com/api/v1/write",
             %{"authorization" => "Bearer token", "x-tenant" => "updated"}}
          ] do
        changeset =
          Adaptor.cast_and_validate_config(
            @subject,
            %{
              url: url,
              headers: %{"Authorization" => "REDACTED", "x-tenant" => "updated"}
            },
            existing
          )

        assert changeset.valid?
        assert Ecto.Changeset.get_field(changeset, :headers) == expected_headers
      end
    end

    test "keeps credentials across a stored round trip through the backend API" do
      user = insert(:user)

      {:ok, backend} =
        Backends.create_backend(user, %{
          name: "vm",
          type: :victoria_metrics,
          config: %{
            url: "https://vm.example.com/api/v1/write",
            username: "user",
            password: "pass",
            headers: %{"authorization" => "Bearer token"},
            labels: %{"env" => "prod"}
          }
        })

      loaded = Backends.get_backend(backend.id)
      assert %{password: "pass", labels: %{"env" => "prod"}} = loaded.config

      {:ok, _updated} =
        Backends.update_backend(loaded, %{config: @subject.redact_config(loaded.config)})

      assert %{password: "pass", headers: %{"authorization" => "Bearer token"}} =
               Backends.get_backend(backend.id).config
    end
  end

  describe "validate_config/1 SSRF" do
    test "rejects private destinations" do
      changeset =
        Adaptor.cast_and_validate_config(@subject, %{
          "url" => "http://127.0.0.1:8428/api/v1/write"
        })

      refute changeset.valid?
      assert {_msg, [validation: :ssrf]} = changeset.errors[:url]
    end

    test "rejects a private read destination independently of the write URL" do
      stub(Logflare.Utils.SSRF, :safe_resolve, fn
        "vm.example.com" -> {:ok, {1, 2, 3, 4}}
        "127.0.0.1" -> {:error, "private address is not allowed"}
      end)

      changeset =
        Adaptor.cast_and_validate_config(@subject, %{
          url: "https://vm.example.com/api/v1/write",
          query_url: "http://127.0.0.1:8428"
        })

      refute changeset.valid?
      assert changeset.errors[:query_url]
      refute changeset.errors[:url]
    end
  end

  describe "redact_config/1" do
    test "redacts the password, credential headers and url userinfo" do
      redacted =
        @subject.redact_config(%{
          url: "https://user:secret@vm.example.com/api/v1/write",
          username: "user",
          password: "secret",
          headers: %{"authorization" => "Bearer token", "x-tenant" => "acme"}
        })

      assert redacted.password == "REDACTED"
      assert redacted.username == "user"
      assert redacted.url == "https://REDACTED@vm.example.com/api/v1/write"
      assert redacted.headers == %{"authorization" => "REDACTED", "x-tenant" => "acme"}
    end

    test "leaves a config without secrets readable" do
      assert %{url: "http://vm:8428/api/v1/write", headers: %{}} =
               redacted = @subject.redact_config(%{url: "http://vm:8428/api/v1/write"})

      refute Map.has_key?(redacted, :password)
    end
  end

  describe "transform_config/1" do
    test "sends one canonical copy of each protocol header" do
      backend =
        build(:backend,
          type: :victoria_metrics,
          config: %{
            url: @vm_remote_write_url,
            username: "user",
            password: "pass",
            headers: %{"Content-Type" => "text/plain", "X-Tenant" => "acme"}
          }
        )

      assert %{headers: headers, gzip: false, http: "http1"} = @subject.transform_config(backend)

      assert headers == %{
               "authorization" => "Basic " <> Base.encode64("user:pass"),
               "content-encoding" => "snappy",
               "content-type" => "application/x-protobuf",
               "x-prometheus-remote-write-version" => "0.1.0",
               "x-tenant" => "acme"
             }
    end
  end

  describe "test_connection/1 (error handling)" do
    setup do
      insert(:plan)
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :victoria_metrics,
          sources: [source],
          config: %{url: "http://vm:8428/api/v1/write"}
        )

      [backend: backend]
    end

    test "returns error on non-2xx response", %{backend: backend} do
      @client
      |> expect(:send, fn _req -> {:ok, %Tesla.Env{status: 401, body: "unauthorized"}} end)

      assert {:error, :http_client_error} = @subject.test_connection(backend)
    end

    test "sends a valid snappy-encoded empty write request", %{backend: backend} do
      test_pid = self()

      @client
      |> expect(:send, fn opts ->
        send(test_pid, {:body, opts[:body]})
        {:ok, %Tesla.Env{status: 204}}
      end)

      assert :ok = @subject.test_connection(backend)
      assert_received {:body, <<0>>}
      assert {:ok, ""} = :snappyer.decompress(<<0>>)
    end

    test "returns error on transport failure", %{backend: backend} do
      @client
      |> expect(:send, fn _req -> {:error, :nxdomain} end)

      assert {:error, :unknown_error} = @subject.test_connection(backend)
    end
  end

  describe "format_batch/1" do
    setup do
      insert(:plan)
      user = insert(:user)
      source = insert(:source, user: user, name: "myservice")
      [source: source]
    end

    test "groups matching series and sorts samples without changing their values", %{
      source: source
    } do
      events =
        for {timestamp, value} <- [
              {1_700_000_002_000_000, 30},
              {1_700_000_000_000_000, 10},
              {1_700_000_001_000_000, 20}
            ] do
          metric_event(source,
            event_message: "queue_depth",
            metric_type: "gauge",
            value: value,
            timestamp: timestamp,
            attributes: %{"queue" => "default"}
          )
        end

      assert [series] = decode(events)

      assert Enum.map(series.samples, &{&1.timestamp, &1.value}) == [
               {1_700_000_000_000, 10.0},
               {1_700_000_001_000, 20.0},
               {1_700_000_002_000, 30.0}
             ]
    end

    test "drops unrepresentable events with a warning and telemetry", %{source: source} do
      attach_drop_handler()

      events = [
        build(:log_event, source: source, event_message: "hello"),
        metric_event(source, metric_type: "exponential_histogram", count: 1),
        metric_event(source,
          metric_type: "sum",
          is_monotonic: true,
          aggregation_temporality: "delta",
          value: 1
        ),
        metric_event(source,
          metric_type: "sum",
          is_monotonic: false,
          aggregation_temporality: "delta",
          value: 3
        ),
        metric_event(source,
          metric_type: "histogram",
          aggregation_temporality: "delta",
          count: 1,
          bucket_counts: [1],
          explicit_bounds: []
        ),
        metric_event(source, event_message: "kept", metric_type: "gauge", value: 1.0)
      ]

      log = capture_log(fn -> assert [_ts] = decode(events) end)

      assert log =~ "Dropping 4 of 6 VictoriaMetrics metric event(s)"
      assert_received {:drop, %{count: 1}, %{reason: :not_a_metric}}
      assert_received {:drop, %{count: 1}, %{reason: :unsupported_type}}
      assert_received {:drop, %{count: 3}, %{reason: :non_cumulative}}
    end

    test "drops malformed data points without losing the batch", %{source: source} do
      attach_drop_handler()

      malformed = [
        [event_message: 123, metric_type: "gauge", value: 1.0],
        [metric_type: "gauge"],
        [metric_type: "gauge", value: "12"],
        [metric_type: "histogram", count: 1, bucket_counts: ["1"]],
        [metric_type: "histogram", count: 2, bucket_counts: [1, 1], explicit_bounds: ["1"]],
        [
          metric_type: "histogram",
          count: 3,
          bucket_counts: [1, 1, 1],
          explicit_bounds: [2.0, 1.0]
        ],
        [metric_type: "histogram", count: 3, bucket_counts: [1, 1, 1], explicit_bounds: [1.0, 1]],
        [metric_type: "histogram", count: 5, bucket_counts: [2, 3], explicit_bounds: [1.0, 2.0]],
        [metric_type: "histogram", count: 1, explicit_bounds: [1.0]],
        [metric_type: "histogram", count: 1, bucket_counts: [5, 0], explicit_bounds: [1.0]],
        [
          metric_type: "histogram",
          count: 9,
          bucket_counts: [1, 2, 3],
          explicit_bounds: [1.0, 2.0]
        ],
        [metric_type: "histogram", count: 1, bucket_counts: [0]],
        [metric_type: "histogram", count: 1, bucket_counts: [-1, 2], explicit_bounds: [1.0]],
        [metric_type: "histogram", count: -1]
      ]

      events =
        Enum.map(malformed, &metric_event(source, &1)) ++
          [metric_event(source, event_message: "kept", metric_type: "gauge", value: 1.0)]

      log =
        capture_log(fn ->
          assert [ts] = decode(events)
          assert label_map(ts)["__name__"] == "kept"
          assert [%{value: 1.0}] = ts.samples
        end)

      assert log =~ "Dropping 14 of 15"
      assert_received {:drop, %{count: 14}, %{reason: :invalid}}
    end

    test "sends NaN, infinite and out-of-range values as IEEE specials", %{source: source} do
      events =
        for {name, value} <- [nan: :nan, inf: :infinity, neg: :negative_infinity, big: 10 ** 400] do
          metric_event(source, event_message: "#{name}", metric_type: "gauge", value: value)
        end

      values = Map.new(decode(events), &{label_map(&1)["__name__"], hd(&1.samples).value})

      assert values == %{
               "nan" => :nan,
               "inf" => :infinity,
               "neg" => :negative_infinity,
               "big" => :infinity
             }
    end

    test "does not warn for log and trace events", %{source: source} do
      attach_drop_handler()
      le = build(:log_event, source: source, event_message: "hello")

      # capture_log also sees other test modules' processes, so only this adaptor's
      # warning is checked for.
      refute capture_log(fn -> assert [] = decode([le]) end) =~ "VictoriaMetrics"
      assert_received {:drop, %{count: 1}, %{reason: :not_a_metric}}
    end

    test "does not warn when every event is sent", %{source: source} do
      le = metric_event(source, metric_type: "gauge", value: 1.0)

      refute capture_log(fn -> assert [_ts] = decode([le]) end) =~ "VictoriaMetrics"
    end
  end

  describe "pipeline" do
    test "acknowledges an entirely dropped batch without making an HTTP request" do
      reject(@client, :send, 1)
      insert(:plan)
      source = insert(:source, user: insert(:user))

      backend =
        insert(:backend,
          type: :victoria_metrics,
          user: source.user,
          sources: [source],
          config: %{url: @vm_remote_write_url}
        )

      start_supervised!({AdaptorSupervisor, {source, backend}})

      event =
        metric_event(source,
          metric_type: "sum",
          aggregation_temporality: "delta",
          value: 1
        )

      log =
        capture_log(fn ->
          ref = Broadway.test_batch(Backends.via_source(source, Pipeline, backend), [event])
          assert_receive {:ack, ^ref, [_message], []}, 5_000
        end)

      assert log =~ "Skipped webhook batch: all 1 log events were dropped"
    end
  end

  defp decode(log_events) do
    log_events
    |> @subject.format_batch()
    |> decode_payload()
  end

  defp decode_payload(payload) do
    {:ok, decompressed} = :snappyer.decompress(payload)
    Prometheus.WriteRequest.decode(decompressed).timeseries
  end

  defp metric_event(source, attrs) do
    defaults = %{source: source, event_message: "metric", metadata: %{"type" => "metric"}}
    build(:log_event, Map.merge(defaults, Map.new(attrs)))
  end

  defp attach_drop_handler do
    test_pid = self()
    handler_id = {__MODULE__, make_ref()}

    :telemetry.attach(
      handler_id,
      [:logflare, :backends, :victoria_metrics, :drop],
      fn _event, measurements, metadata, _config ->
        send(test_pid, {:drop, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  defp label_map(timeseries) do
    Map.new(timeseries.labels, fn %{name: k, value: v} -> {k, v} end)
  end
end
