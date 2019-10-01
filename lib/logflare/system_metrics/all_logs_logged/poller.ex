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

  def logs_last_second_cluster() do
    nodes = Phoenix.Tracker.list(Logflare.Tracker, __MODULE__)

    Enum.map(nodes, fn {_x, y} -> y.last_second end)
    |> Enum.sum()
  end

  def total_logs_logged_cluster() do
    nodes = Phoenix.Tracker.list(Logflare.Tracker, __MODULE__)

    init_inserts =
      Enum.map(nodes, fn {_x, y} -> y.init_total end)
      |> Enum.sum()

    inserts_since_init =
      Enum.map(nodes, fn {_x, y} -> y.inserts_since_init end)
      |> Enum.sum()

    old_nodes_total_logs_logged() + init_inserts + inserts_since_init
  end

  def old_nodes_total_logs_logged() do
    all_nodes_metrics = Logflare.Repo.all(Logflare.SystemMetric)

    current_nodes =
      Cluster.Utils.node_list_all()
      |> Enum.map(fn k -> Atom.to_string(k) end)
      |> MapSet.new()

    all_nodes_ever =
      all_nodes_metrics
      |> MapSet.new(fn v -> v.node end)

    old_nodes = MapSet.difference(all_nodes_ever, current_nodes)

    Enum.reduce(all_nodes_metrics, fn %{node: node, all_logs_logged: x}, acc ->
      if MapSet.member?(old_nodes, node) do
        %{acc | all_logs_logged: x + acc.all_logs_logged}
      else
        %{acc | all_logs_logged: acc.all_logs_logged}
      end
    end)
    |> Map.get(:all_logs_logged)
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

    Phoenix.Tracker.track(Logflare.Tracker, self(), __MODULE__, Node.self(), state)

    {:ok, state}
  end

  def handle_info(:poll_per_second, state) do
    poll_per_second()

    {:ok, metrics} = AllLogsLogged.all_metrics(:total_logs_logged)
    logs_last_second = metrics.total - state.last_total

    state = %{state | last_second: logs_last_second, last_total: metrics.total}

    Phoenix.Tracker.update(Logflare.Tracker, self(), __MODULE__, Node.self(), state)

    log_stuff(logs_last_second, metrics)
    {:noreply, state}
  end

  def handle_info(:poll_total_logs, state) do
    poll_total_logs()

    {:ok, metrics} = AllLogsLogged.all_metrics(:total_logs_logged)

    state = %{state | inserts_since_init: metrics.inserts_since_init}

    Phoenix.Tracker.update(Logflare.Tracker, self(), __MODULE__, Node.self(), state)

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
