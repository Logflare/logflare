defmodule LogflareTelemetry.Aggregators.V0.Redix do
  @moduledoc """
  Aggregates Ecto telemetry metrics
  """
  use GenServer
  alias LogflareTelemetry, as: LT
  alias LT.MetricsCache
  alias LT.Aggregators.GenAggregator
  alias LT.Config

  def start_link(config, opts \\ []) do
    GenServer.start_link(__MODULE__, config, name: __MODULE__)
  end

  @impl true
  def init(%Config{} = config) do
    {:ok, %{config: config}}
  end

  @impl true
  def handle_info(:tick, %{config: %Config{} = config} = state) do
    for metric <- config.metrics do
      {:ok, value} =
        case metric do
          _ ->
            MetricsCache.get(metric)
        end

      GenAggregator.dispatch(metric, value, config)
      MetricsCache.reset(metric)
    end

    Process.send_after(self(), :tick, config.tick_interval)
    {:noreply, state}
  end
end
