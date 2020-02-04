defmodule LogflareTelemetry.Reporters.V0.BEAM do
  @moduledoc """
  Custom LogflareTelemetry reporter for handling BEAM telemetry events
  """
  use GenServer
  require Logger
  @env Application.get_env(:logflare, :env)
  alias LogflareTelemetry.MetricsCache
  alias LogflareTelemetry, as: LT
  alias LT.ExtendedMetrics, as: ExtMetrics

  def start_link(config) do
    GenServer.start_link(__MODULE__, config)
  end

  def init(config) do
    if @env != :test do
      Process.flag(:trap_exit, true)
    end

    attach_handlers(config.metrics)

    {:ok, %{}}
  end

  def attach_handlers(metrics) do
    metrics
    |> Enum.group_by(& &1.event_name)
    |> Enum.each(fn {event, metrics} ->
      id = {__MODULE__, event, self()}
      :telemetry.attach(id, event, &handle_event/4, metrics)
    end)
  end

  def handle_event(_event_name, measurements, metadata, metrics) do
    Enum.map(metrics, &handle_metric(&1, measurements, metadata))
  end

  defp extract_measurement(metric, measurements) do
    case metric.measurement do
      fun when is_function(fun, 1) -> fun.(measurements)
      key -> measurements[key]
    end
  end

  def handle_metric(%ExtMetrics.LastValues{} = metric, measurements, _metadata) do
    measurement = extract_measurement(metric, measurements)
    MetricsCache.put(metric, measurement)
  end

  def terminate(_, events) do
    Enum.each(events, &:telemetry.detach({__MODULE__, &1, self()}))
    :ok
  end
end
