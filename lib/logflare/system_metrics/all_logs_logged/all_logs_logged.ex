defmodule Logflare.SystemMetrics.AllLogsLogged do
  @moduledoc false
  use GenServer

  require Logger

  alias Logflare.Repo
  alias Logflare.SystemMetric
  alias Logflare.Cluster

  @total_logs :total_logs_logged
  @table :system_counter
  @persist_every 5_000

  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args, name: __MODULE__)
  end

  @doc """
  Creates the counter table, then seeds it from the database.

  This process owns `#{inspect(@table)}`, so the table is created before anything
  that can raise: a crash here takes the table with it, and every ingest then
  fails in `increment/2` until the table is recreated. Seeding is best effort for
  the same reason - an unreachable database must not stop the counter working.
  """
  def init(state) do
    create(@total_logs, 0)
    persist()

    {:ok, state, {:continue, :seed_log_count}}
  end

  def handle_continue(:seed_log_count, state) do
    case with_repo(fn -> Repo.get_by(SystemMetric, node: node_name()) end) do
      {:ok, %SystemMetric{all_logs_logged: count}} ->
        :ets.update_counter(@table, @total_logs, {3, count}, {@total_logs, 0, 0})

      {:ok, nil} ->
        :ok

      {:error, error} ->
        Logger.warning(
          "Could not seed #{@total_logs} from the database, counting from 0: " <>
            Exception.message(error)
        )
    end

    {:noreply, state}
  end

  def handle_info(:persist, state) do
    {:ok, log_count} = log_count(@total_logs)

    with {:error, error} <-
           with_repo(fn ->
             insert_or_update_node_metric(%{all_logs_logged: log_count, node: node_name()})
           end) do
      Logger.warning("Could not persist #{@total_logs}: " <> Exception.message(error))
    end

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

  @spec increment(atom()) :: {:ok, atom()}
  @spec increment(atom(), non_neg_integer()) :: {:ok, atom()}
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

  # this process owns the counter table, so a raise here would destroy it and break
  # every subsequent `increment/2` until the database recovers
  defp with_repo(fun) do
    {:ok, fun.()}
  rescue
    error in [DBConnection.ConnectionError] -> {:error, error}
  end
end
