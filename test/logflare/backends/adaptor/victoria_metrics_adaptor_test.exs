defmodule Logflare.Backends.Adaptor.VictoriaMetricsAdaptorTest do
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.AdaptorSupervisor
  alias Logflare.SystemMetrics.AllLogsLogged

  @subject Logflare.Backends.Adaptor.VictoriaMetricsAdaptor
  @client Logflare.Backends.Adaptor.WebhookAdaptor.Client

  # docker-compose `vm` service — see docker-compose.yml
  @vm_base_url "http://localhost:8428"
  @vm_remote_write_url @vm_base_url <> "/api/v1/write"
  # Freshly written samples take a few seconds to become searchable in VM.
  @vm_retry [sleep: 250, duration: 30_000]

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

    test "gauge event produces single TimeSeries", %{source: source} do
      le =
        metric_event(source,
          event_message: "http.server.duration",
          metric_type: "gauge",
          value: 42.5,
          timestamp: 1_700_000_000_000_000_000,
          attributes: %{"method" => "GET", "status" => "200"}
        )

      assert [ts] = decode([le])
      assert [sample] = ts.samples
      assert sample.value == 42.5
      assert sample.timestamp == 1_700_000_000_000

      assert %{
               "__name__" => "http_server_duration",
               "source" => "myservice",
               "method" => "GET",
               "status" => "200"
             } = label_map(ts)
    end

    test "sum event produces single TimeSeries", %{source: source} do
      le = metric_event(source, event_message: "requests_total", metric_type: "sum", value: 100)

      assert [ts] = decode([le])
      assert label_map(ts)["__name__"] == "requests_total"
      assert [%{value: 100.0}] = ts.samples
    end

    test "histogram event produces _count, _sum, and cumulative _bucket series",
         %{source: source} do
      le =
        metric_event(source,
          event_message: "latency",
          metric_type: "histogram",
          count: 10,
          sum: 500.0,
          bucket_counts: [2, 5, 3],
          explicit_bounds: [100.0, 500.0]
        )

      timeseries = decode([le])
      names = Enum.map(timeseries, &label_map(&1)["__name__"])

      assert "latency_count" in names
      assert "latency_sum" in names
      assert "latency_bucket" in names

      assert %{
               "100" => [%{value: 2.0}],
               "500" => [%{value: 7.0}],
               "+Inf" => [%{value: 10.0}]
             } = buckets(timeseries, "latency_bucket")
    end

    test "formats le bounds as plain decimals", %{source: source} do
      le =
        metric_event(source,
          event_message: "latency",
          metric_type: "histogram",
          count: 4,
          sum: 1.0,
          bucket_counts: [1, 1, 1, 1, 0],
          explicit_bounds: [1.0e-9, 0.005, 2.5, 1000.0]
        )

      assert ["+Inf", "0.000000001", "0.005", "1000", "2.5"] =
               [le] |> decode() |> buckets("latency_bucket") |> Map.keys() |> Enum.sort()
    end

    test "maps service resource attributes to job and instance", %{source: source} do
      le =
        metric_event(source,
          metric_type: "gauge",
          value: 1.0,
          resource: %{
            "service.name" => "checkout",
            "service.namespace" => "shop",
            "service.instance.id" => "checkout-1"
          }
        )

      assert [ts] = decode([le])
      assert %{"job" => "shop/checkout", "instance" => "checkout-1"} = label_map(ts)
    end

    test "uses service.name alone as job when there is no namespace", %{source: source} do
      le =
        metric_event(source,
          metric_type: "gauge",
          value: 1.0,
          resource: %{"service.name" => "checkout"}
        )

      assert [ts] = decode([le])
      labels = label_map(ts)
      assert labels["job"] == "checkout"
      refute Map.has_key?(labels, "instance")
    end

    test "keeps colliding attributes as exported_ labels", %{source: source} do
      le =
        metric_event(source,
          metric_type: "gauge",
          value: 1.0,
          resource: %{"service.name" => "checkout"},
          attributes: %{"source" => "client", "job" => "batch", "region" => "eu"}
        )

      assert [ts] = decode([le])

      assert %{
               "source" => "myservice",
               "job" => "checkout",
               "exported_source" => "client",
               "exported_job" => "batch",
               "region" => "eu"
             } = label_map(ts)
    end

    test "skips list and map attribute values instead of crashing", %{source: source} do
      le =
        metric_event(source,
          event_message: "jobs",
          metric_type: "gauge",
          value: 1.0,
          attributes: %{
            "queue" => "default",
            "retry" => true,
            "ratio" => 1000.0,
            "tags" => ["a", 0.3],
            "http" => %{"method" => "GET"}
          }
        )

      assert [ts] = decode([le])
      labels = label_map(ts)

      assert %{"queue" => "default", "retry" => "true", "ratio" => "1000"} = labels
      refute Map.has_key?(labels, "tags")
      refute Map.has_key?(labels, "http")
    end

    test "sanitizes metric and label names into Prometheus identifiers", %{source: source} do
      le =
        metric_event(source,
          event_message: "9lives.req/sec-é:x",
          metric_type: "gauge",
          value: 1.0,
          attributes: %{"http.route" => "/", "1st" => "a"}
        )

      assert [ts] = decode([le])
      labels = label_map(ts)

      assert labels["__name__"] == "_9lives_req_sec___:x"
      # LogEvent.make/2 has already normalized the attribute keys
      assert labels["_http_route"] == "/"
      assert labels["_1st"] == "a"
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
        [metric_type: "histogram", count: -1]
      ]

      events =
        Enum.map(malformed, &metric_event(source, &1)) ++
          [metric_event(source, event_message: "kept", metric_type: "gauge", value: 1.0)]

      log = capture_log(fn -> assert [_ts] = decode(events) end)

      assert log =~ "Dropping 9 of 10"
      assert_received {:drop, %{count: 9}, %{reason: :invalid}}
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

    test "leaves out _sum when the histogram has no sum", %{source: source} do
      le =
        metric_event(source,
          event_message: "latency",
          metric_type: "histogram",
          count: 3,
          bucket_counts: [1, 2],
          explicit_bounds: [1.0]
        )

      names = [le] |> decode() |> Enum.map(&label_map(&1)["__name__"]) |> Enum.uniq()

      assert Enum.sort(names) == ["latency_bucket", "latency_count"]
    end

    test "always sends a +Inf bucket holding the count", %{source: source} do
      without_buckets =
        metric_event(source, event_message: "no_buckets", metric_type: "histogram", count: 4)

      inconsistent_overflow =
        metric_event(source,
          event_message: "overflow",
          metric_type: "histogram",
          count: 9,
          bucket_counts: [1, 2, 3],
          explicit_bounds: [1.0, 2.0]
        )

      timeseries = decode([without_buckets, inconsistent_overflow])

      assert %{"+Inf" => [%{value: 4.0}]} = buckets(timeseries, "no_buckets_bucket")

      assert %{"1" => [%{value: 1.0}], "2" => [%{value: 3.0}], "+Inf" => [%{value: 9.0}]} =
               buckets(timeseries, "overflow_bucket")
    end

    test "does not warn for log and trace events", %{source: source} do
      attach_drop_handler()
      le = build(:log_event, source: source, event_message: "hello")

      assert capture_log(fn -> assert [] = decode([le]) end) == ""
      assert_received {:drop, %{count: 1}, %{reason: :not_a_metric}}
    end

    test "skips empty label names and values", %{source: source} do
      le = metric_event(source, metric_type: "gauge", value: 1.0, attributes: %{"kept" => "x"})

      [ts] =
        [le]
        |> @subject.format_batch(%{labels: %{"" => "x", "empty" => ""}})
        |> decode_payload()

      labels = label_map(ts)
      assert labels["kept"] == "x"
      refute Map.has_key?(labels, "")
      refute Map.has_key?(labels, "empty")
    end

    test "prefixes exported_ again when the exported name is taken", %{source: source} do
      le =
        metric_event(source,
          metric_type: "gauge",
          value: 1.0,
          attributes: %{"source" => "client", "exported_source" => "proxy"}
        )

      assert [ts] = decode([le])

      assert %{
               "source" => "myservice",
               "exported_source" => "proxy",
               "exported_exported_source" => "client"
             } = label_map(ts)
    end

    test "does not warn when every event is sent", %{source: source} do
      le = metric_event(source, metric_type: "gauge", value: 1.0)

      assert capture_log(fn -> assert [_ts] = decode([le]) end) == ""
    end
  end

  describe "format_batch/2" do
    test "merges config labels into every series, overriding attributes" do
      insert(:plan)
      source = insert(:source, user: insert(:user))

      le =
        metric_event(source,
          event_message: "jobs",
          metric_type: "gauge",
          value: 1.0,
          attributes: %{"env" => "staging", "queue" => "default"}
        )

      [ts] =
        [le]
        |> @subject.format_batch(%{labels: %{"env" => "prod", "deploy.region" => "eu"}})
        |> decode_payload()

      assert %{"env" => "prod", "deploy_region" => "eu", "queue" => "default"} = label_map(ts)
    end

    test "keeps config labels with reserved names as exported_ labels" do
      insert(:plan)
      source = insert(:source, user: insert(:user), name: "configured")
      le = metric_event(source, metric_type: "gauge", value: 1.0)

      [ts] =
        [le]
        |> @subject.format_batch(%{labels: %{"job" => "static", "source" => "static"}})
        |> decode_payload()

      labels = label_map(ts)
      assert %{"source" => "configured", "exported_job" => "static"} = labels
      assert labels["exported_source"] == "static"
      refute Map.has_key?(labels, "job")
    end
  end

  # Runs in CI: drives the real Broadway pipeline and captures the HTTP request at the
  # client boundary, so no VictoriaMetrics instance is needed.
  describe "pipeline" do
    setup :set_mimic_global

    setup do
      insert(:plan)
      source = insert(:source, user: insert(:user), name: "pipeline_source")

      backend =
        insert(:backend,
          type: :victoria_metrics,
          sources: [source],
          config: %{url: @vm_remote_write_url, labels: %{"env" => "test"}}
        )

      start_supervised!({AdaptorSupervisor, {source, backend}})
      [source: source]
    end

    test "sends metric batches as snappy remote write requests", %{source: source} do
      test_pid = self()

      expect(@client, :send, fn opts ->
        send(test_pid, {:sent, opts})
        {:ok, %Tesla.Env{status: 204}}
      end)

      le =
        metric_event(source,
          event_message: "pipeline.gauge",
          metric_type: "gauge",
          value: 2.0,
          attributes: %{"queue" => "default"}
        )

      assert {:ok, _} = Backends.ingest_logs([le], source)
      assert_receive {:sent, opts}, 5_000

      assert opts[:url] == @vm_remote_write_url
      assert opts[:gzip] == false
      assert opts[:headers]["content-encoding"] == "snappy"

      assert [ts] = decode_payload(opts[:body])

      assert %{
               "__name__" => "pipeline_gauge",
               "source" => "pipeline_source",
               "queue" => "default",
               "env" => "test"
             } = label_map(ts)
    end
  end

  # End-to-end tests against the docker-compose `vm` service.
  #
  # Requires `docker compose up -d vm`. Excluded by default via the
  # :integration tag (see test/test_helper.exs).
  #
  # Run with:
  #   mix test test/logflare/backends/adaptor/victoria_metrics_adaptor_test.exs --include integration
  describe "victoriametrics e2e" do
    @describetag :integration

    # The vm service is on loopback, which SSRFProtection blocks. The pipeline sends
    # from its own processes, so the stub has to be global.
    setup :set_mimic_global

    setup do
      stub(Logflare.Utils.SSRF, :safe_resolve, fn _ -> {:ok, {127, 0, 0, 1}} end)

      insert(:plan)
      user = insert(:user)

      source =
        insert(:source, user: user, name: "vm_e2e_#{System.unique_integer([:positive])}")

      backend =
        insert(:backend,
          type: :victoria_metrics,
          sources: [source],
          config: %{url: @vm_remote_write_url}
        )

      start_supervised!({AdaptorSupervisor, {source, backend}})
      [source: source, backend: backend]
    end

    test "test_connection/1 succeeds against the running VM service",
         %{backend: backend} do
      assert :ok = @subject.test_connection(backend)
    end

    test "metrics flow through the pipeline and are queryable",
         %{source: source} do
      metric_name = "logflare_e2e_#{System.unique_integer([:positive])}"
      expected_value = 42.0

      le =
        build(:log_event,
          source: source,
          event_message: metric_name,
          metric_type: "gauge",
          value: expected_value,
          timestamp: System.system_time(:nanosecond),
          metadata: %{"type" => "metric"},
          attributes: %{"env" => "test"}
        )

      assert {:ok, _} = Backends.ingest_logs([le], source)

      TestUtils.retry_assert(@vm_retry, fn ->
        assert [%{"value" => ^expected_value, "source" => src, "env" => "test"} | _] =
                 query_vm(metric_name)

        assert src == source.name
      end)
    end

    # The adaptor sanitizes Prometheus identifiers (replacing `.`, `/`, `-`
    # etc. with `_`). Verify by ingesting under the raw name and asserting
    # that VM has the series under the sanitized name.
    test "metric names are sanitized into Prometheus identifiers",
         %{source: source} do
      suffix = System.unique_integer([:positive])
      raw_name = "logflare.e2e/duration-ms_#{suffix}"
      sanitized = "logflare_e2e_duration_ms_#{suffix}"

      le =
        build(:log_event,
          source: source,
          event_message: raw_name,
          metric_type: "gauge",
          value: 7.0,
          timestamp: System.system_time(:nanosecond),
          metadata: %{"type" => "metric"}
        )

      assert {:ok, _} = Backends.ingest_logs([le], source)

      TestUtils.retry_assert(@vm_retry, fn ->
        assert [%{"__name__" => ^sanitized} | _] = query_vm(sanitized)
      end)
    end

    # Prometheus remote write v1 has no native encoding for exponential
    # histograms, so the adaptor drops them. Verify nothing reaches VM by
    # ingesting a control gauge alongside, waiting for the gauge to appear,
    # then asserting the exponential_histogram series is absent.
    test "exponential_histogram events are dropped during ingestion",
         %{source: source} do
      suffix = System.unique_integer([:positive])
      exp_name = "logflare_e2e_exp_#{suffix}"
      control_name = "logflare_e2e_control_#{suffix}"
      ts = System.system_time(:nanosecond)

      exp_event =
        build(:log_event,
          source: source,
          event_message: exp_name,
          metric_type: "exponential_histogram",
          timestamp: ts,
          metadata: %{"type" => "metric"}
        )

      control_event =
        build(:log_event,
          source: source,
          event_message: control_name,
          metric_type: "gauge",
          value: 1.0,
          timestamp: ts,
          metadata: %{"type" => "metric"}
        )

      assert {:ok, _} = Backends.ingest_logs([exp_event, control_event], source)

      # Wait until the control gauge is visible — that means VM has flushed
      # this batch, so anything missing now was dropped, not just late.
      TestUtils.retry_assert(@vm_retry, fn ->
        assert [_ | _] = query_vm(control_name)
      end)

      assert query_vm(exp_name) == []
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

  defp buckets(timeseries, name) do
    for ts <- timeseries, labels = label_map(ts), labels["__name__"] == name, into: %{} do
      {labels["le"], ts.samples}
    end
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

  # Runs a PromQL instant query against the docker-compose VM service and returns
  # each series' labels with its sample value under "value". VM hides samples newer
  # than 30s from queries by default, so the latency offset is lowered.
  defp query_vm(query) do
    params = URI.encode_query(%{"query" => query, "latency_offset" => "1ms"})
    url = @vm_base_url <> "/api/v1/query?" <> params
    {:ok, %{status_code: 200, body: body}} = HTTPoison.get(url)
    %{"data" => %{"result" => result}} = Jason.decode!(body)

    for %{"metric" => labels, "value" => [_ts, value]} <- result do
      {value, ""} = Float.parse(value)
      Map.put(labels, "value", value)
    end
  end
end
