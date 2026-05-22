defmodule Logflare.Backends.Adaptor.VictoriaMetricsAdaptorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
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
    test "url is required" do
      refute Adaptor.cast_and_validate_config(@subject, %{}).valid?
    end

    test "valid with url only" do
      assert Adaptor.cast_and_validate_config(@subject, %{"url" => "http://vm:8428/api/v1/write"}).valid?
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

  describe "test_connection/1" do
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

    test "POSTs a snappy-compressed empty WriteRequest", %{backend: backend} do
      @client
      |> expect(:send, fn req ->
        assert req[:url] == "http://vm:8428/api/v1/write"
        assert req[:headers]["Content-Type"] == "application/x-protobuf"
        assert req[:headers]["Content-Encoding"] == "snappy"
        assert req[:headers]["X-Prometheus-Remote-Write-Version"] == "0.1.0"

        {:ok, decompressed} = :snappyer.decompress(req[:body])
        decoded = Prometheus.WriteRequest.decode(decompressed)
        assert decoded.timeseries == []

        {:ok, %Tesla.Env{status: 204, body: ""}}
      end)

      assert :ok = @subject.test_connection(backend)
    end

    test "returns error on non-2xx response", %{backend: backend} do
      @client
      |> expect(:send, fn _req -> {:ok, %Tesla.Env{status: 401, body: "unauthorized"}} end)

      assert {:error, reason} = @subject.test_connection(backend)
      assert reason =~ "401"
    end

    test "returns error on transport failure", %{backend: backend} do
      @client
      |> expect(:send, fn _req -> {:error, :nxdomain} end)

      assert {:error, reason} = @subject.test_connection(backend)
      assert reason =~ "nxdomain"
    end
  end

  describe "format_batch/1" do
    test "drops non-metric events" do
      le = build(:log_event, event_message: "hello")
      result = @subject.format_batch([le])

      {:ok, decompressed} = :snappyer.decompress(result)
      decoded = Prometheus.WriteRequest.decode(decompressed)
      assert decoded.timeseries == []
    end

    test "gauge event produces single TimeSeries" do
      insert(:plan)
      user = insert(:user)
      source = insert(:source, user: user, name: "myservice")

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

      result = @subject.format_batch([le])
      {:ok, decompressed} = :snappyer.decompress(result)
      decoded = Prometheus.WriteRequest.decode(decompressed)

      assert [ts] = decoded.timeseries
      assert [sample] = ts.samples
      assert sample.value == 42.5
      assert sample.timestamp == 1_700_000_000_000

      label_map = Map.new(ts.labels, fn %{name: k, value: v} -> {k, v} end)
      assert label_map["__name__"] == "http_server_duration"
      assert label_map["source"] == "myservice"
      assert label_map["method"] == "GET"
      assert label_map["status"] == "200"
    end

    test "sum event produces single TimeSeries" do
      insert(:plan)
      user = insert(:user)
      source = insert(:source, user: user)

      le =
        build(:log_event,
          source: source,
          event_message: "requests_total",
          metric_type: "sum",
          value: 100,
          timestamp: 1_700_000_000_000_000_000,
          metadata: %{"type" => "metric"}
        )

      result = @subject.format_batch([le])
      {:ok, decompressed} = :snappyer.decompress(result)
      decoded = Prometheus.WriteRequest.decode(decompressed)

      assert [ts] = decoded.timeseries
      label_map = Map.new(ts.labels, fn %{name: k, value: v} -> {k, v} end)
      assert label_map["__name__"] == "requests_total"
      assert [%{value: 100.0}] = ts.samples
    end

    test "histogram event produces _count, _sum, and _bucket series" do
      insert(:plan)
      user = insert(:user)
      source = insert(:source, user: user)

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

      result = @subject.format_batch([le])
      {:ok, decompressed} = :snappyer.decompress(result)
      decoded = Prometheus.WriteRequest.decode(decompressed)

      names =
        Enum.map(decoded.timeseries, fn ts ->
          Map.new(ts.labels, fn %{name: k, value: v} -> {k, v} end)["__name__"]
        end)

      assert "latency_count" in names
      assert "latency_sum" in names
      assert "latency_bucket" in names

      bucket_series =
        Enum.filter(decoded.timeseries, fn ts ->
          Map.new(ts.labels, fn %{name: k, value: v} -> {k, v} end)["__name__"] == "latency_bucket"
        end)

      le_values =
        Enum.map(bucket_series, fn ts ->
          Map.new(ts.labels, fn %{name: k, value: v} -> {k, v} end)["le"]
        end)

      assert "100.0" in le_values
      assert "500.0" in le_values
      assert "+Inf" in le_values

      inf_ts =
        Enum.find(bucket_series, fn ts ->
          Map.new(ts.labels, fn %{name: k, value: v} -> {k, v} end)["le"] == "+Inf"
        end)

      assert [%{value: 10.0}] = inf_ts.samples
    end

    test "metric name is sanitized" do
      insert(:plan)
      user = insert(:user)
      source = insert(:source, user: user)

      le =
        build(:log_event,
          source: source,
          event_message: "http.server/duration-ms",
          metric_type: "gauge",
          value: 1.0,
          timestamp: 1_700_000_000_000_000_000,
          metadata: %{"type" => "metric"}
        )

      result = @subject.format_batch([le])
      {:ok, decompressed} = :snappyer.decompress(result)
      decoded = Prometheus.WriteRequest.decode(decompressed)

      [ts] = decoded.timeseries
      label_map = Map.new(ts.labels, fn %{name: k, value: v} -> {k, v} end)
      assert label_map["__name__"] == "http_server_duration_ms"
    end

    # Prometheus remote write v1 has no native encoding for exponential
    # histograms (introduced in OTLP for sparse high-resolution histograms),
    # so the adaptor drops them rather than emit a misleading series.
    test "exponential_histogram events are dropped" do
      insert(:plan)
      user = insert(:user)
      source = insert(:source, user: user)

      le =
        build(:log_event,
          source: source,
          event_message: "exp_hist",
          metric_type: "exponential_histogram",
          timestamp: 1_700_000_000_000_000_000,
          metadata: %{"type" => "metric"}
        )

      result = @subject.format_batch([le])
      {:ok, decompressed} = :snappyer.decompress(result)
      decoded = Prometheus.WriteRequest.decode(decompressed)
      assert decoded.timeseries == []
    end
  end

  # End-to-end test against the docker-compose `vm` service.
  #
  # Requires `docker compose up -d vm` and is excluded by default
  # (see :integration tag in test/test_helper.exs).
  #
  # Run with:
  #   mix test test/logflare/backends/adaptor/victoria_metrics_adaptor_test.exs --include integration
  describe "victoriametrics ingestion (e2e)" do
    @describetag :integration

    setup do
      # The global Mimic.stub(Finch) in test_helper.exs intercepts all Finch
      # calls. Override with passthrough so the Broadway pipeline can actually
      # POST to the docker-compose `vm` service.
      Mimic.stub(Finch, :build, fn m, u, h, b ->
        Mimic.call_original(Finch, :build, [m, u, h, b])
      end)

      Mimic.stub(Finch, :build, fn m, u, h, b, o ->
        Mimic.call_original(Finch, :build, [m, u, h, b, o])
      end)

      Mimic.stub(Finch, :request, fn r, n ->
        Mimic.call_original(Finch, :request, [r, n])
      end)

      Mimic.stub(Finch, :request, fn r, n, o ->
        Mimic.call_original(Finch, :request, [r, n, o])
      end)

      insert(:plan)
      user = insert(:user)
      source = insert(:source, user: user, name: "vm_e2e_test_#{System.unique_integer([:positive])}")

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

    test "metrics flow through the pipeline and are queryable via execute_query/3",
         %{source: source, backend: backend} do
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

      assert eventually(fn ->
               case @subject.execute_query(backend, metric_name) do
                 {:ok, %{rows: [%{"value" => value, "source" => src, "env" => env} | _]}} ->
                   value == expected_value and src == source.name and env == "test"

                 _ ->
                   false
               end
             end),
             "metric #{metric_name} was not visible in VictoriaMetrics within timeout"
    end
  end

  defp eventually(fun, timeout_ms \\ 5_000, interval_ms \\ 200) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_eventually(fun, deadline, interval_ms)
  end

  defp do_eventually(fun, deadline, interval_ms) do
    case fun.() do
      true ->
        true

      _ ->
        if System.monotonic_time(:millisecond) < deadline do
          Process.sleep(interval_ms)
          do_eventually(fun, deadline, interval_ms)
        else
          false
        end
    end
  end
end
