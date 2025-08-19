defmodule Logflare.SystemMetrics.AllLogsLogged do
  @moduledoc false
  use GenServer

  alias Logflare.Repo
  alias Logflare.SystemMetric
  alias Logflare.Cluster

  require Logger

  @total_logs :total_logs_logged
  @table :system_counter
  @persist_every 5_000

  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args, name: __MODULE__)
  end

  def init(state) do
    case Repo.get_by(SystemMetric, node: node_name()) do
      nil ->
        create(@total_logs, 0)

      total_logs ->
        create(@total_logs, total_logs.all_logs_logged)
    end

    persist()

    {:ok, state}
  end

  def handle_info(:persist, state) do
    {:ok, log_count} = log_count(@total_logs)

    insert_or_update_node_metric(%{all_logs_logged: log_count, node: node_name()})
    persist(@persist_every * Cluster.Utils.actual_cluster_size())
    {:noreply, state}
  end

  ## Public Functions

  @spec create(atom, integer()) :: {:ok, atom}
  def create(metric, count \\ 0) do
    :ets.new(@table, [:public, :named_table])

    :ets.update_counter(@table, metric, {2, 0}, {metric, 0, 0})
    :ets.update_counter(@table, metric, {3, count}, {metric, 0, 0})

    {:ok, metric}
  end

  @spec increment(atom) :: {:ok, atom}
  @spec increment(atom, non_neg_integer()) :: {:ok, atom}
  def increment(metric, n \\ 1) do
    :ets.update_counter(@table, metric, {2, n}, {metric, 0, 0})

    {:ok, metric}
  end

  @spec log_count(atom) :: {:ok, non_neg_integer}
  def log_count(metric) do
    [{_metric, inserts_since_init, init_log_count}] = :ets.lookup(@table, metric)
    count = inserts_since_init + init_log_count

    {:ok, count}
  end

  def init_log_count(metric) do
    [{_metric, _inserts_since_init, init_log_count}] = :ets.lookup(@table, metric)

    {:ok, init_log_count}
  end

  def all_metrics(metric) do
    [{_metric, inserts_since_init, init_log_count}] = :ets.lookup(@table, metric)
    total = inserts_since_init + init_log_count

    {:ok, %{inserts_since_init: inserts_since_init, init_log_count: init_log_count, total: total}}
  end

  ## Private Functions

  defp node_name do
    Atom.to_string(node())
  end

  defp insert_or_update_node_metric(params) do
    case Repo.get_by(SystemMetric, node: node_name()) do
      nil ->
        changeset = SystemMetric.changeset(%SystemMetric{}, params)

        Repo.insert(changeset)

      metric ->
        changeset = SystemMetric.changeset(metric, params)

        Repo.update(changeset)
    end
  end

  defp persist(every \\ @persist_every) do
    Process.send_after(self(), :persist, every)
  end
end
