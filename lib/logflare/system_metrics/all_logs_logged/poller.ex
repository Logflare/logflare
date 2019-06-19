defmodule Logflare.SystemMetrics.AllLogsLogged.Poller do
  use GenServer

  alias Logflare.SystemMetrics.AllLogsLogged

  @poll_every 1_000

  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args, name: __MODULE__)
  end

  def get_total_logs_per_second() do
    GenServer.call(__MODULE__, :logs_last_second)
  end

  def init(_state) do
    poll_metrics()
    {:ok, all_logs_logged} = AllLogsLogged.log_count(:total_logs_logged)
    {:ok, %{last_total: all_logs_logged, last_second: 0}}
  end

  def handle_info(:poll_metrics, state) do
    poll_metrics()
    {:ok, all_logs_logged} = AllLogsLogged.log_count(:total_logs_logged)
    logs_last_second = all_logs_logged - state.last_total

    LogflareLogger.info("All logs logged!", all_logs_logged: all_logs_logged)
    LogflareLogger.info("Logs last second!", logs_per_second: logs_last_second)

    {:noreply, %{last_total: all_logs_logged, last_second: logs_last_second}}
  end

  def handle_call(:logs_last_second, _from, state), do: {:reply, state.last_second, state}

  defp poll_metrics() do
    Process.send_after(self(), :poll_metrics, @poll_every)
  end
end
