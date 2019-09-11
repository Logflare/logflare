defmodule LogflareTelemetry.Reporters.Ecto.V0 do
  use GenServer
  require Logger
  @env Application.get_env(:logflare, :env)
  alias LogflareTelemetry, as: LT
  alias LT.Reporters.Gen.V0, as: Reporter
  alias LT.Reporters.Ecto.Transformer.V0, as: Transformer
  alias LT.{MetricsCache, LogflareMetrics}

  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts[:metrics])
  end

  def init(metrics) do
    Logger.info("Logflare Telemetry Ecto Reporter is being initialized...")

    if @env != :test do
      Process.flag(:trap_exit, true)
    end

    attach_handlers(metrics)

    {:ok, %{}}
  end

  def attach_handlers(metrics) do
    Logger.debug("Logflare Telemetry Ecto Reporter is attaching handlers")

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

  def handle_metric(%LogflareMetrics.All{} = metric, measurements, metadata) do
    tele_event =
      metadata
      |> Transformer.prepare_metadata()
      |> Map.merge(%{
        measurements: measurements
      })

    MetricsCache.push(metric, tele_event)
  end

  def handle_metric(metric, measurements, metadata) do
    Reporter.handle_metric(metric, measurements, metadata)
  end

  def terminate(_, events) do
    Enum.each(events, &:telemetry.detach({__MODULE__, &1, self()}))
    :ok
  end
end
