defmodule Logflare.SystemMetrics.AllLogsLogged.Poller do
  @moduledoc false
  use GenServer

  require Logger

  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.Cluster

  @poll_per_second 1_000
  @poll_total_every 1_000

  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args, name: __MODULE__)
  end

  def get_total_logs_per_second() do
    GenServer.call(__MODULE__, :logs_last_second)
  end

  def init(_state) do
    poll_per_second()
    poll_total_logs()

    {:ok, metrics} = AllLogsLogged.all_metrics(:total_logs_logged)

    state = %{
      init_total: metrics.init_log_count,
      last_total: metrics.total,
      inserts_since_init: metrics.inserts_since_init,
      last_second: 0
    }

    {:ok, state}
  end

  def handle_info(:poll_per_second, state) do
    poll_per_second()

    {:ok, metrics} = AllLogsLogged.all_metrics(:total_logs_logged)
    logs_last_second = metrics.total - state.last_total

    state = %{state | last_second: logs_last_second, last_total: metrics.total}

    log_stuff(logs_last_second, metrics)
    {:noreply, state}
  end

  def handle_info(:poll_total_logs, state) do
    poll_total_logs()

    {:ok, metrics} = AllLogsLogged.all_metrics(:total_logs_logged)

    state = %{state | inserts_since_init: metrics.inserts_since_init}

    {:noreply, state}
  end

  def handle_call(:logs_last_second, _from, state), do: {:reply, state.last_second, state}

  defp poll_per_second() do
    Process.send_after(self(), :poll_per_second, @poll_per_second)
  end

  defp poll_total_logs() do
    Process.send_after(self(), :poll_total_logs, @poll_total_every)
  end

  defp log_stuff(logs_last_second, metrics) do
    if Application.get_env(:logflare, :env) == :prod do
      Logger.info("All logs logged!", all_logs_logged: metrics.total)
      Logger.info("Logs last second!", logs_per_second: logs_last_second)
    end
  end
end
