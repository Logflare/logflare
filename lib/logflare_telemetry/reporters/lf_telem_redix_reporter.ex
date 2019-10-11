defmodule LogflareTelemetry.Reporters.V0.Redix do
  require Logger
  alias LogflareTelemetry, as: LT
  alias LT.ExtendedMetrics, as: ExtMetrics
  alias LogflareTelemetry.MetricsCache
  use GenServer

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def init(config) do
    attach_handlers(config.metrics)

    {:ok, config}
  end

  def attach_handlers(metrics) do
    metrics
    |> Enum.group_by(& &1.event_name)
    |> Enum.each(fn {event, metrics} ->
      id = {__MODULE__, event, self()}
      :telemetry.attach(id, event, &handle_event/4, metrics)
    end)
  end

  def handle_event(_event, measurements, metadata, metrics) do
    Enum.map(metrics, &handle_metric(&1, measurements, metadata))
  end

  def handle_metric(%ExtMetrics.Every{} = metric, measurements, _metadata) do
    measurements =
      measurements
      |> Map.update!(:elapsed_time, &System.convert_time_unit(&1, :native, :microsecond))

    MetricsCache.push(metric, measurements)
  end

  def handle_metric(%ExtMetrics.Every{} = metric, measurements, _metadata) do
    MetricsCache.push(metric, measurements)
  end

  def terminate(_, events) do
    Enum.each(events, &:telemetry.detach({__MODULE__, &1, self()}))
    :ok
  end
end
