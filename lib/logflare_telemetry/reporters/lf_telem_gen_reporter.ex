defmodule LogflareTelemetry.Reporters.Gen.V0 do
  use GenServer
  require Logger
  @env Application.get_env(:logflare, :env)

  alias Telemetry.Metrics.{Counter, Distribution, LastValue, Sum, Summary}
  alias LogflareTelemetry.MetricsCache

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts[:metrics])
  end

  def init(metrics) do
    if @env != :test do
      Process.flag(:trap_exit, true)
    end

    attach_handlers(metrics)

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

  def handle_metric(%Counter{} = metric, _measurements, _metadata) do
    MetricsCache.increment(metric)
  end

  def handle_metric(%Sum{} = metric, measurements, _metadata) do
    MetricsCache.increment(metric, extract_measurement(metric, measurements))
  end

  def handle_metric(%LastValue{} = metric, measurements, _metadata) do
    MetricsCache.put(metric, extract_measurement(metric, measurements))
  end

  def handle_metric(%Distribution{} = metric, measurements, _metadata) do
    MetricsCache.push(metric, extract_measurement(metric, measurements))
  end

  def handle_metric(%Summary{} = metric, measurements, _metadata) do
    MetricsCache.push(metric, extract_measurement(metric, measurements))
  end

  def terminate(_, events) do
    Enum.each(events, &:telemetry.detach({__MODULE__, &1, self()}))
    :ok
  end
end
