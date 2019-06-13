defmodule Logflare.SystemMetrics.Observer.PerSecond do
  use GenServer

  alias Logflare.SystemMetrics.Observer

  require Logger

  @send_every 1_000
  @collect_procs_for 1_000

  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args, name: __MODULE__)
  end

  def init(state) do
    poll_metrics()
    {:ok, state}
  end

  def handle_info(:poll_metrics, state) do
    poll_metrics()
    observer_metrics = Observer.get_metrics()
    processes = Observer.Procs.top(@collect_procs_for)
    observer_memory = Observer.get_memory()

    LogflareLogger.info("Memory metrics!", observer_memory: observer_memory)
    LogflareLogger.info("Process metrics!", processes: processes)
    LogflareLogger.info("Observer metrics!", observer_metrics: observer_metrics)

    {:noreply, state}
  end

  defp poll_metrics() do
    Process.send_after(self(), :poll_metrics, @send_every)
  end
end
