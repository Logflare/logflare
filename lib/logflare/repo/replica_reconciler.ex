defmodule Logflare.Repo.ReplicaReconciler do
  @moduledoc """
  Periodically verifies that each configured read replica is in sync with the primary.

  A dedicated, locally registered worker runs on every node, because the disabled state is
  node-local. With no replicas configured, each run is a no-op.

  For each of `users`, `sources`, `backends` and `rules`, the row count and `max(updated_at)` of rows
  last updated before a lag cutoff are compared between the primary and each replica. Rows
  touched inside the lag window are ignored so in-flight pglogical writes do not trigger a
  mismatch, and a mismatch is re-checked once before acting on it.

  A replica that stays out of sync is disabled on this node only, so `Logflare.Repo.with_replica/1`
  stops routing reads to it. A later passing check re-enables it. Disabled replica keys are kept
  in `:persistent_term`, which is only written when the set changes.
  """

  use GenServer

  require Logger

  alias Ecto.Adapters.SQL
  alias Logflare.Repo
  alias Logflare.Repo.Replicas

  @tables ~w(users sources backends rules)
  @disabled_key {__MODULE__, :disabled}
  @default_interval :timer.minutes(30)
  @default_jitter :timer.minutes(5)
  @default_initial_delay :timer.minutes(1)
  @default_lag :timer.minutes(1)
  @default_recheck_delay :timer.seconds(30)

  @type replica_key :: String.t() | module()
  @type mismatch :: %{
          table: String.t(),
          primary: {non_neg_integer(), NaiveDateTime.t() | nil},
          replica: {non_neg_integer(), NaiveDateTime.t() | nil}
        }
  @type check_result :: :ok | {:mismatch, [mismatch()]} | {:error, term()}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc """
  Returns the keys of replicas disabled on this node.
  """
  @spec disabled_replicas() :: MapSet.t(replica_key())
  def disabled_replicas, do: :persistent_term.get(@disabled_key, MapSet.new())

  @doc """
  Returns whether the replica with the given key is disabled on this node.
  """
  @spec disabled?(replica_key()) :: boolean()
  def disabled?(key), do: MapSet.member?(disabled_replicas(), key)

  @doc """
  Returns the key identifying a `:read_replicas` entry.
  """
  @spec replica_key(module() | {String.t(), keyword()}) :: replica_key()
  def replica_key({key, _config}), do: key
  def replica_key(repo) when is_atom(repo), do: repo

  @doc """
  Checks every configured replica and updates the disabled set accordingly.
  """
  @spec reconcile(keyword()) :: %{replica_key() => check_result()}
  def reconcile(opts \\ []) do
    :logflare
    |> Application.fetch_env!(:read_replicas)
    |> Map.new(fn entry ->
      key = replica_key(entry)
      result = check_replica(key, resolve_repo(entry), opts)
      apply_result(key, result)
      {key, result}
    end)
  end

  @doc """
  Compares one replica against the primary, re-checking once after `:recheck_delay` on mismatch.
  """
  @spec check_replica(replica_key(), Ecto.Repo.t() | pid(), keyword()) :: check_result()
  def check_replica(key, replica_repo, opts \\ []) do
    case compare(replica_repo, opts) do
      {:mismatch, _mismatches} ->
        Logger.warning("Read replica out of sync, re-checking", replica: inspect(key))
        Process.sleep(Keyword.get(opts, :recheck_delay, @default_recheck_delay))
        compare(replica_repo, opts)

      result ->
        result
    end
  end

  @doc """
  Compares the reconciled tables of a replica against the primary, ignoring rows updated within
  the `:lag` window.
  """
  @spec compare(Ecto.Repo.t() | pid(), keyword()) :: check_result()
  def compare(replica_repo, opts \\ []) do
    cutoff =
      NaiveDateTime.add(
        NaiveDateTime.utc_now(),
        -Keyword.get(opts, :lag, @default_lag),
        :millisecond
      )

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
  def init(opts) do
    state = Map.new(opts)
    schedule(Map.get(state, :initial_delay, @default_initial_delay), state)
    {:ok, state}
  end

  @impl true
  def handle_info(:reconcile, state) do
    state |> Map.to_list() |> reconcile()
    schedule(Map.get(state, :interval, @default_interval), state)
    {:noreply, state}
  end

  defp schedule(delay, state) do
    jitter = Map.get(state, :jitter, @default_jitter)
    Process.send_after(self(), :reconcile, delay + :rand.uniform(jitter + 1) - 1)
  end

  defp table_stats(repo, cutoff) do
    sql =
      Enum.map_join(@tables, " UNION ALL ", fn table ->
        "SELECT '#{table}', count(*), max(updated_at) FROM #{table} WHERE updated_at <= $1"
      end)

    case SQL.query(repo, sql, [cutoff]) do
      {:ok, %{rows: rows}} ->
        {:ok,
         Map.new(rows, fn [table, count, max_updated_at] -> {table, {count, max_updated_at}} end)}

      {:error, error} ->
        {:error, error}
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
