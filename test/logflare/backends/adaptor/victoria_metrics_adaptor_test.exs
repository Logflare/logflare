defmodule Logflare.Backends.Adaptor.VictoriaMetricsAdaptorTest do
  use Logflare.DataCase, async: false

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
  end

  describe "redact_config/1" do
    test "redacts password when present" do
      assert %{password: "REDACTED"} =
               @subject.redact_config(%{password: "secret", url: "http://vm:8428/api/v1/write"})
    end

    test "leaves config unchanged when password absent" do
      config = %{url: "http://vm:8428/api/v1/write"}
      assert ^config = @subject.redact_config(config)
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

    test "drops non-metric events" do
      le = build(:log_event, event_message: "hello")

      assert decode([le]) == []
    end

    test "gauge event produces single TimeSeries", %{source: source} do
      le =
        build(:log_event,
          source: source,
          event_message: "http.server.duration",
          metric_type: "gauge",
          value: 42.5,
          timestamp: 1_700_000_000_000_000_000,
          metadata: %{"type" => "metric"},
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
      le =
        build(:log_event,
          source: source,
          event_message: "requests_total",
          metric_type: "sum",
          value: 100,
          timestamp: 1_700_000_000_000_000_000,
          metadata: %{"type" => "metric"}
        )

      assert [ts] = decode([le])
      assert label_map(ts)["__name__"] == "requests_total"
      assert [%{value: 100.0}] = ts.samples
    end

    test "histogram event produces _count, _sum, and _bucket series", %{source: source} do
      le =
        build(:log_event,
          source: source,
          event_message: "latency",
          metric_type: "histogram",
          count: 10,
          sum: 500.0,
          bucket_counts: [2, 5, 3],
          explicit_bounds: [100.0, 500.0],
          timestamp: 1_700_000_000_000_000_000,
          metadata: %{"type" => "metric"}
        )

      timeseries = decode([le])
      names = Enum.map(timeseries, &label_map(&1)["__name__"])

      assert "latency_count" in names
      assert "latency_sum" in names
      assert "latency_bucket" in names

      buckets =
        timeseries
        |> Enum.map(&{label_map(&1), &1.samples})
        |> Enum.filter(fn {labels, _samples} -> labels["__name__"] == "latency_bucket" end)
        |> Map.new(fn {labels, samples} -> {labels["le"], samples} end)

      assert %{
               "100.0" => [%{value: 2.0}],
               "500.0" => [%{value: 7.0}],
               "+Inf" => [%{value: 10.0}]
             } = buckets
    end

    test "skips list and map attribute values instead of crashing", %{source: source} do
      le =
        build(:log_event,
          source: source,
          event_message: "jobs",
          metric_type: "gauge",
          value: 1.0,
          metadata: %{"type" => "metric"},
          attributes: %{
            "queue" => "default",
            "retry" => true,
            "tags" => ["a", 0.3],
            "http" => %{"method" => "GET"}
          }
        )

      assert [ts] = decode([le])
      labels = label_map(ts)

      assert %{"queue" => "default", "retry" => "true"} = labels
      refute Map.has_key?(labels, "tags")
      refute Map.has_key?(labels, "http")
    end
  end

  describe "format_batch/2" do
    test "merges config labels into every series, overriding attributes" do
      insert(:plan)
      source = insert(:source, user: insert(:user))

      le =
        build(:log_event,
          source: source,
          event_message: "jobs",
          metric_type: "gauge",
          value: 1.0,
          metadata: %{"type" => "metric"},
          attributes: %{"env" => "staging", "queue" => "default"}
        )

      [ts] =
        [le]
        |> @subject.format_batch(%{labels: %{"env" => "prod", "deploy.region" => "eu"}})
        |> decode_payload()

      assert %{"env" => "prod", "deploy_region" => "eu", "queue" => "default"} = label_map(ts)
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
      :timer.sleep(500)
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
