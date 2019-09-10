defmodule LogflareTelemetry.Aggregators.BEAM.V0 do
  @moduledoc """
  Aggregates Ecto telemetry metrics
  """
  use GenServer
  alias LogflareTelemetry.MetricsCache
  alias LogflareTelemetry.BQBackend
  alias Telemetry.Metrics.{Counter, LastValue, Sum}
  @backend Logflare.TelemetryBackend.BQ

  defmodule Config do
    @moduledoc false
    defstruct [:tick_interval, :metrics]
  end

  def start_link(args, opts \\ []) do
    config = struct!(Config, args)
    GenServer.start_link(__MODULE__, config, opts)
  end

  @impl true
  def init(%Config{} = config) do
    Process.send_after(self(), :tick, config.tick_interval)
    {:ok, %{config: config}}
  end

  @impl true
  def handle_info(:tick, %{config: %Config{} = config} = state) do
    for metric <- config.metrics do
      {:ok, value} =
        case metric do
          %Counter{} ->
            MetricsCache.get(metric)

          %Sum{} ->
            MetricsCache.get(metric)

          %LastValue{} ->
            MetricsCache.get(metric)
        end

      if value do
        :ok = @backend.ingest(metric, value)
        MetricsCache.reset(metric)
      end
    end

    Process.send_after(self(), :tick, config.tick_interval)
    {:noreply, state}
  end
end
