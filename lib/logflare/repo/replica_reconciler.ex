defmodule Logflare.Repo.ReplicaReconciler do
  @moduledoc """
  Runs on every node and periodically compares the row count and `max(updated_at)` of key
  tables between the primary and each read replica, ignoring rows updated within the
  replication lag window.

  An out-of-sync replica is disabled on this node only, so `Logflare.Repo.with_replica/1` skips
  it. A later passing check re-enables it. The disabled set is kept in `:persistent_term`.
  """

  use GenServer

  require Logger

  alias Ecto.Adapters.SQL
  alias Logflare.Repo
  alias Logflare.Repo.Replicas

  @tables ~w(users sources backends rules)
  @disabled_key {__MODULE__, :disabled}
  @interval :timer.minutes(30)
  @jitter :timer.minutes(5)
  @initial_delay :timer.minutes(1)
  @lag_seconds 60

  @type replica_key :: String.t() | module()
  @type stats :: {non_neg_integer(), NaiveDateTime.t() | nil}
  @type mismatch :: %{table: String.t(), primary: stats(), replica: stats()}
  @type check_result :: :ok | {:mismatch, [mismatch()]} | {:error, term()}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @spec disabled_replicas() :: MapSet.t(replica_key())
  def disabled_replicas, do: :persistent_term.get(@disabled_key, MapSet.new())

  @spec disabled?(replica_key()) :: boolean()
  def disabled?(key), do: MapSet.member?(disabled_replicas(), key)

  @spec replica_key(module() | {String.t(), keyword()}) :: replica_key()
  def replica_key({key, _config}), do: key
  def replica_key(repo) when is_atom(repo), do: repo

  @doc """
  Checks every configured replica and updates the disabled set.
  """
  @spec reconcile() :: %{replica_key() => check_result()}
  def reconcile do
    :logflare
    |> Application.fetch_env!(:read_replicas)
    |> Map.new(fn entry ->
      key = replica_key(entry)
      result = compare(resolve_repo(entry))
      apply_result(key, result)
      {key, result}
    end)
  end

  @doc """
  Compares the reconciled tables of a replica against the primary.
  """
  @spec compare(Ecto.Repo.t() | pid()) :: check_result()
  def compare(replica_repo) do
    cutoff = NaiveDateTime.add(NaiveDateTime.utc_now(), -@lag_seconds)

    with {:ok, primary} <- table_stats(Repo, cutoff),
         {:ok, replica} <- table_stats(replica_repo, cutoff) do
      case Enum.reject(@tables, &(primary[&1] == replica[&1])) do
        [] ->
          :ok

        tables ->
          {:mismatch, Enum.map(tables, &%{table: &1, primary: primary[&1], replica: replica[&1]})}
      end
    end
  end

  @impl true
  def init(_opts) do
    schedule(@initial_delay)
    {:ok, nil}
  end

  @impl true
  def handle_info(:reconcile, state) do
    reconcile()
    schedule(@interval)
    {:noreply, state}
  end

  defp schedule(delay), do: Process.send_after(self(), :reconcile, delay + :rand.uniform(@jitter))

  defp table_stats(repo, cutoff) do
    sql =
      Enum.map_join(@tables, " UNION ALL ", fn table ->
        "SELECT '#{table}', count(*), max(updated_at) FROM #{table} WHERE updated_at <= $1"
      end)

    with {:ok, %{rows: rows}} <- SQL.query(repo, sql, [cutoff]) do
      {:ok,
       Map.new(rows, fn [table, count, max_updated_at] -> {table, {count, max_updated_at}} end)}
    end
  end

  defp apply_result(key, :ok) do
    if disabled?(key) do
      Logger.warning("Read replica back in sync, re-enabling", replica: inspect(key))
      put_disabled(MapSet.delete(disabled_replicas(), key))
    end
  end

  defp apply_result(key, {:mismatch, mismatches}) do
    for %{table: table, primary: {p_count, p_max}, replica: {r_count, r_max}} <- mismatches do
      Logger.error("Read replica out of sync with primary, disabling replica reads on this node",
        replica: inspect(key),
        table: table,
        primary_count: p_count,
        replica_count: r_count,
        primary_max_updated_at: inspect(p_max),
        replica_max_updated_at: inspect(r_max)
      )
    end

    unless disabled?(key), do: put_disabled(MapSet.put(disabled_replicas(), key))
  end

  defp apply_result(key, {:error, error}) do
    Logger.warning("Read replica reconciliation skipped: #{inspect(error)}",
      replica: inspect(key)
    )
  end

  defp put_disabled(disabled), do: :persistent_term.put(@disabled_key, disabled)

  defp resolve_repo(repo) when is_atom(repo), do: repo
  defp resolve_repo({key, _config}), do: Replicas.lookup!(key)
end
