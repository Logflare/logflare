defmodule Logflare.SystemMetrics do
  use GenServer

  alias Logflare.SystemMetrics.{Observer, Procs, AllLogsLogged}

  require Logger

  @send_every 1_000
  @collect_procs_for 1_000

  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(state) do
    send_it()
    {:ok, state}
  end

  def handle_info(:send_it, state) do
    send_it()
    observer_metrics = Observer.get_metrics()
    processes = Procs.top(@collect_procs_for)
    {:ok, all_logs_logged} = AllLogsLogged.log_count(:total_logs_logged)

    LogflareLogger.info("Process metrics!", processes: processes)
    LogflareLogger.info("Observer metrics!", observer_metrics: observer_metrics)
    LogflareLogger.info("All logs logged!", all_logs_logged: all_logs_logged)

    {:noreply, state}
  end

  defp send_it() do
    Process.send_after(self(), :send_it, @send_every)
  end
end
