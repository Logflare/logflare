defmodule Logflare.TelemetryIngestDispatchExportTest do
  use Logflare.DataCase, async: false

  import Logflare.TelemetryExportTestHelpers

  alias Logflare.Backends
  alias Logflare.Backends.SourceSup
  alias Logflare.Telemetry

  setup do
    start_supervised!(Logflare.SystemMetrics.AllLogsLogged)
    :ok
  end

  for consolidated? <- [false, true] do
    @consolidated consolidated?
    test "exports dispatch counts when targeting an explicit backend (consolidated=#{consolidated?})",
         %{test: name} do
      metric_names = [
        "logflare.backends.ingest.dispatch.count",
        "logflare.backends.ingest.dispatch.stop.duration"
      ]

      metrics = Enum.filter(Telemetry.metrics(), &(Enum.join(&1.name, ".") in metric_names))
      assert MapSet.new(metrics, &Enum.join(&1.name, ".")) == MapSet.new(metric_names)
      start_exporter(metrics, name)
      insert(:plan)
      source = insert(:source, user: insert(:user))
      start_supervised!({SourceSup, source})

      backend = %{
        build(:backend, type: :webhook)
        | id: System.unique_integer([:positive]),
          consolidated_ingest?: @consolidated
      }

      queue_key =
        if @consolidated,
          do: {:consolidated, backend.id, self()},
          else: {source.id, backend.id, self()}

      assert {:ok, _tid} = IngestEventQueue.upsert_tid(queue_key)

      try do
        assert {:ok, 2} =
                 Backends.ingest_logs(
                   [build(:log_event, source: source), build(:log_event, source: source)],
                   source,
                   backend
                 )

        batch = export_metrics(name)
        assert MapSet.new(batch, & &1.name) == MapSet.new(metric_names)
        exported = Map.new(batch, &{&1.name, &1})

        assert {:sum, %{data_points: [count_point]}} =
                 exported["logflare.backends.ingest.dispatch.count"].data

        assert count_point.value == {:as_int, 2}
        assert point_attributes(count_point) == %{"backend_type" => "webhook"}
        duration = exported["logflare.backends.ingest.dispatch.stop.duration"]
        assert duration.unit == "ms"
        assert {:histogram, %{data_points: [duration_point]}} = duration.data
        assert duration_point.count == 1
        assert duration_point.sum >= 0
        assert point_attributes(duration_point) == %{"backend_type" => "webhook"}
      after
        IngestEventQueue.delete_queue(queue_key)
        IngestEventQueue.prune_generations({elem(queue_key, 0), elem(queue_key, 1)})
      end
    end
  end
end
