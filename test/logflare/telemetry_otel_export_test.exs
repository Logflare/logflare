defmodule Logflare.TelemetryOtelExportTest do
  use ExUnit.Case, async: false
  use Mimic

  import Logflare.TelemetryExportTestHelpers
  import Telemetry.Metrics, only: [counter: 1]

  alias Logflare.SystemMetrics.Cluster
  alias Logflare.TestUtils
  alias OtelMetricExporter.MetricStore

  setup :set_mimic_private

  test "exports HTTP metrics from real endpoint requests", %{test: name} do
    finch_name = __MODULE__.Finch

    metrics =
      Enum.filter(Logflare.Telemetry.metrics(), fn %{event_name: [prefix | _]} ->
        prefix in [:bandit, :phoenix, :thousand_island, :finch]
      end)

    start_exporter(metrics, name)

    start_supervised!(
      {Finch,
       name: finch_name,
       pools: %{default: [protocols: [:http1], size: 1, count: 1, conn_max_idle_time: 0]}}
    )

    bandit_options = [
      plug: LogflareWeb.Endpoint,
      ip: {127, 0, 0, 1},
      port: 0,
      startup_log: false
    ]

    server = start_supervised!({Bandit, bandit_options}, id: :bandit)
    {:ok, {_address, port}} = ThousandIsland.listener_info(server)
    request = Finch.build(:get, "http://127.0.0.1:#{port}/api/openapi")

    for _ <- 1..2 do
      assert {:ok, %Finch.Response{status: 200}} =
               Finch.request(request, finch_name, receive_timeout: 5_000)
    end

    stop_supervised!(:bandit)

    server =
      start_supervised!(
        {Bandit,
         Keyword.put(bandit_options, :thousand_island_options,
           num_acceptors: 1,
           num_connections: 0,
           max_connections_retry_count: 0
         )}
      )

    {:ok, {_address, port}} = ThousandIsland.listener_info(server)
    request = Finch.build(:get, "http://127.0.0.1:#{port}/api/openapi")
    assert {:error, _reason} = Finch.request(request, finch_name, receive_timeout: 5_000)

    TestUtils.retry_assert(fn ->
      assert %{
               {:counter, "logflare.total_http_requests"} => %{%{} => 2},
               {:counter, "thousand_island.acceptor.spawn_error"} => %{%{} => 1}
             } = MetricStore.get_metrics(name)
    end)

    batch = export_metrics(name)
    assert MapSet.new(batch, & &1.name) == MapSet.new(metrics, &Enum.join(&1.name, "."))
    exported = Map.new(batch, &{&1.name, &1})

    for {metric_name, count} <- [
          {"logflare.total_http_requests", 2},
          {"thousand_island.acceptor.spawn_error", 1},
          {"finch.conn_max_idle_time_exceeded.idle_time", 1}
        ] do
      assert {:sum, sum} = exported[metric_name].data
      assert sum.is_monotonic
      assert [%{value: {:as_int, ^count}}] = sum.data_points
    end

    for {metric_name, count} <- [
          {"phoenix.endpoint.stop.duration", 2},
          {"phoenix.router_dispatch.stop.duration", 2},
          {"finch.request.stop.duration", 3},
          {"finch.connect.stop.duration", 3},
          {"finch.queue.stop.duration", 3}
        ] do
      assert exported[metric_name].unit == "ms"
      assert {:histogram, histogram} = exported[metric_name].data
      assert [%{count: ^count, sum: duration}] = histogram.data_points
      assert duration >= 0
    end
  end

  for protocol <- [:http1, :http2] do
    test "exports active and idle #{protocol} Finch pool requests", %{test: name} do
      assert [metric] =
               Enum.filter(
                 Logflare.Telemetry.metrics(),
                 &(&1.name == [:logflare, :system, :finch, :in_flight_requests])
               )

      start_exporter([metric], name)
      test_pid = self()
      finch_name = __MODULE__.Finch

      server =
        start_supervised!(
          {Bandit,
           plug: fn conn, _opts ->
             send(test_pid, {:request_started, self()})

             receive do
               :respond -> Plug.Conn.send_resp(conn, 200, "ok")
             end
           end,
           ip: {127, 0, 0, 1},
           port: 0,
           startup_log: false}
        )

      {:ok, {_address, port}} = ThousandIsland.listener_info(server)
      url = "http://127.0.0.1:#{port}"

      start_supervised!(
        {Finch,
         name: finch_name,
         pools: %{
           url => [protocols: [unquote(protocol)], count: 1, size: 1, start_pool_metrics?: true]
         }}
      )

      stub(Finch, :get_pool_status, fn
        Logflare.FinchDefault, "https://bigquery.googleapis.com" ->
          Mimic.call_original(Finch, :get_pool_status, [finch_name, {:http, "127.0.0.1", port}])

        _pool, _url ->
          {:error, :not_found}
      end)

      request = Task.async(fn -> Finch.request(Finch.build(:get, url), finch_name) end)
      assert_receive {:request_started, connection}, 5_000

      tags = %{
        pool: Atom.to_string(Logflare.FinchDefault),
        url: "https://bigquery.googleapis.com"
      }

      try do
        Cluster.finch()
        assert_in_flight_requests(name, tags, 1)
      after
        send(connection, :respond)
      end

      assert {:ok, %Finch.Response{status: 200}} = Task.await(request)

      TestUtils.retry_assert(fn ->
        Cluster.finch()

        assert %{{:last_value, "logflare.system.finch.in_flight_requests"} => values} =
                 MetricStore.get_metrics(name)

        assert values[tags] == 0
      end)

      assert_in_flight_requests(name, tags, 0)
    end
  end

  describe "OTel resource export" do
    test "emits the build commit SHA as a service resource attribute" do
      resource = export_resource("deadbeef")

      assert resource["service.name"] == "Logflare"
      assert resource["service.commit"] == "deadbeef"
    end

    test "omits the commit attribute when the SHA is an empty string" do
      # An unset Docker build-arg expands `ENV LOGFLARE_COMMIT_SHA=${COMMIT_SHA}`
      # to "", so the var is present but empty in real deployments.
      resource = export_resource("")

      assert resource["service.name"] == "Logflare"
      refute Map.has_key?(resource, "service.commit")
    end

    test "omits the commit attribute when no SHA is set" do
      resource = export_resource(nil)

      assert resource["service.name"] == "Logflare"
      refute Map.has_key?(resource, "service.commit")
    end
  end

  @spec assert_in_flight_requests(atom(), map(), non_neg_integer()) :: :ok
  defp assert_in_flight_requests(exporter, tags, expected) do
    assert [%{name: "logflare.system.finch.in_flight_requests", data: {:gauge, gauge}}] =
             export_metrics(exporter)

    assert [point] = gauge.data_points

    assert point_attributes(point) ==
             Map.new(tags, fn {key, value} -> {Atom.to_string(key), value} end)

    assert {:as_double, value} = point.value
    assert value == expected
    :ok
  end

  # Builds the OTel resource via Telemetry.resource/0, runs it through a real
  # exporter, and returns the resource as it would be emitted downstream.
  defp export_resource(commit_sha) do
    test_pid = self()

    if commit_sha do
      System.put_env("LOGFLARE_COMMIT_SHA", commit_sha)
    else
      System.delete_env("LOGFLARE_COMMIT_SHA")
    end

    on_exit(fn -> System.delete_env("LOGFLARE_COMMIT_SHA") end)

    name = :"telemetry_otel_export_#{System.unique_integer([:positive])}"

    start_supervised!(
      {OtelMetricExporter,
       name: name,
       metrics: [counter("#{name}.count")],
       export_period: to_timeout(minute: 5),
       export_callback: fn {type, _batch}, config ->
         send(test_pid, {:otel_export, type, config.resource})
         :ok
       end,
       resource: Logflare.Telemetry.resource()}
    )

    assert :ok = MetricStore.export_sync(name)
    assert_receive {:otel_export, :metrics, resource}

    resource
  end
end
