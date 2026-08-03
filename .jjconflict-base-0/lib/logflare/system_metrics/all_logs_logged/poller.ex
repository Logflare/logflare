defmodule Logflare.SystemMetrics.AllLogsLogged.Poller do
  @moduledoc false
  use GenServer

  alias Logflare.Cluster
  alias Logflare.Repo
  alias Logflare.SystemMetric
  alias Logflare.SystemMetrics.AllLogsLogged

  require Logger

  @poll_per_second 1_000

  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args, name: __MODULE__)
  end

  def get_logs_per_second_cluster do
    {success, _failed} =
      GenServer.multi_call(Cluster.Utils.node_list_all(), __MODULE__, :logs_last_second, 5_000)

    for {_node, count} <- success do
      count
    end
    |> Enum.sum()
  end

  def get_total_logs_per_second do
    GenServer.call(__MODULE__, :logs_last_second)
  end

  def total_logs_logged_cluster do
    SystemMetric
    |> Repo.all()
    |> Enum.map(fn x -> x.all_logs_logged end)
    |> Enum.sum()
  end

  def init(_state) do
    {:ok, metrics} = AllLogsLogged.all_metrics(:total_logs_logged)

    state = %{
      init_total: metrics.init_log_count,
      last_total: metrics.total,
      inserts_since_init: metrics.inserts_since_init,
      last_second: 0
    }

    poll_per_second()

    {:ok, state}
  end

  def handle_info({_ref, {:ok, _id}}, state) do
    # Getting messages from tracker here for some reason.
    # Logflare.SystemMetrics.AllLogsLogged.Poller.handle_info(
    # {#Reference<0.2515045418.1638137859.219426>, {:ok, "Fl6TQMxzPJKX90GH"}},
    #               %{init_total: 1384387391, inserts_since_init: 93034149, last_second: 472, last_total: 1480439241})
    Logger.warning("Handle Tracker message.")

    {:noreply, state}
  end

  def handle_info(:poll_per_second, state) do
    {:ok, metrics} = AllLogsLogged.all_metrics(:total_logs_logged)
    logs_last_second = metrics.total - state.last_total
    state = %{state | last_second: logs_last_second, last_total: metrics.total}

    log_stuff(logs_last_second)

    poll_per_second()

    {:noreply, state}
  end

  def handle_call(:logs_last_second, _from, state), do: {:reply, state.last_second, state}

  defp poll_per_second do
    Process.send_after(self(), :poll_per_second, @poll_per_second)
  end

  defp log_stuff(logs_last_second) do
    if Application.get_env(:logflare, :env) == :prod do
      Logger.debug("All logs logged!", all_logs_logged: total_logs_logged_cluster())
      Logger.debug("Logs last second!", logs_per_second: logs_last_second)
    end
  end
end
