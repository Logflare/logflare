defmodule Logflare.SystemMetrics.AllLogsLogged do
  @moduledoc false
  use GenServer

  alias Logflare.Cluster
  alias Logflare.Repo
  alias Logflare.SystemMetric

  @total_logs :total_logs_logged
  @table :system_counter
  @inserts_since_init 1
  @init_log_count 2
  @counter_count 2
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

  @spec create(atom(), integer()) :: {:ok, atom()}
  def create(metric, count \\ 0) do
    :ets.new(@table, [:public, :named_table, read_concurrency: true])
    ref = new_counter(count)
    true = :ets.insert(@table, {metric, ref})

    {:ok, metric}
  end

  @spec increment(atom()) :: {:ok, atom()}
  @spec increment(atom(), non_neg_integer()) :: {:ok, atom()}
  def increment(metric, n \\ 1) do
    metric
    |> counter_ref()
    |> :counters.add(@inserts_since_init, n)

    {:ok, metric}
  end

  @spec log_count(atom()) :: {:ok, non_neg_integer()}
  def log_count(metric) do
    ref = fetch_counter_ref!(metric)
    count = :counters.get(ref, @inserts_since_init) + :counters.get(ref, @init_log_count)

    {:ok, count}
  end

  @spec init_log_count(atom()) :: {:ok, non_neg_integer()}
  def init_log_count(metric) do
    {:ok, metric |> fetch_counter_ref!() |> :counters.get(@init_log_count)}
  end

  @spec all_metrics(atom()) ::
          {:ok,
           %{
             inserts_since_init: non_neg_integer(),
             init_log_count: non_neg_integer(),
             total: non_neg_integer()
           }}
  def all_metrics(metric) do
    ref = fetch_counter_ref!(metric)
    inserts_since_init = :counters.get(ref, @inserts_since_init)
    init_log_count = :counters.get(ref, @init_log_count)
    total = inserts_since_init + init_log_count

    {:ok, %{inserts_since_init: inserts_since_init, init_log_count: init_log_count, total: total}}
  end

  ## Private Functions

  @spec counter_ref(atom()) :: :counters.counters_ref()
  defp counter_ref(metric) do
    case :ets.lookup(@table, metric) do
      [{^metric, ref}] -> ref
      [] -> insert_counter_ref(metric)
    end
  end

  @spec insert_counter_ref(atom()) :: :counters.counters_ref()
  defp insert_counter_ref(metric) do
    ref = new_counter(0)

    if :ets.insert_new(@table, {metric, ref}) do
      ref
    else
      counter_ref(metric)
    end
  end

  @spec fetch_counter_ref!(atom()) :: :counters.counters_ref()
  defp fetch_counter_ref!(metric) do
    [{^metric, ref}] = :ets.lookup(@table, metric)
    ref
  end

  @spec new_counter(non_neg_integer()) :: :counters.counters_ref()
  defp new_counter(init_log_count) do
    ref = :counters.new(@counter_count, [:write_concurrency])
    :counters.put(ref, @init_log_count, init_log_count)
    ref
  end

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
