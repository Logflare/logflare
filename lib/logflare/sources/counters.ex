defmodule Logflare.Sources.Counters do
  @moduledoc false
  @callback get_inserts(atom) :: {:ok, integer}
  alias Logflare.Sources.Source
  use GenServer

  require Logger

  @ets_table_name :table_counters
  @inserts 1
  @deletes 2
  @bq_inserts 3
  @inserts_since_boot 4
  @total_cluster_inserts 5
  @source_changed_at 6
  @counter_count 6

  @type success_tuple :: {:ok, atom}

  def start_link(args \\ []) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def init(state) do
    Process.flag(:trap_exit, true)

    :ets.new(@ets_table_name, [:public, :named_table, read_concurrency: true])
    {:ok, state}
  end

  def terminate(reason, _state) do
    Logger.warning("[#{__MODULE__}] terminating - #{reason} ")
    reason
  end

  @spec create(atom) :: success_tuple()
  def create(table) do
    _ref = counter_ref(table)
    {:ok, table}
  end

  @spec increment(atom) :: success_tuple()
  @spec increment(atom, non_neg_integer()) :: success_tuple()
  def increment(table, n \\ 1) do
    add(table, @inserts, n)
  end

  @spec increment_bq_count(atom, non_neg_integer) :: success_tuple()
  def increment_bq_count(table, count) do
    add(table, @bq_inserts, count)
  end

  @spec increment_inserts_since_boot_count(atom, non_neg_integer) :: success_tuple()
  def increment_inserts_since_boot_count(table, count) do
    add(table, @inserts_since_boot, count)
  end

  @spec increment_total_cluster_inserts_count(atom, non_neg_integer) :: success_tuple()
  def increment_total_cluster_inserts_count(table, count) do
    add(table, @total_cluster_inserts, count)
  end

  @spec increment_source_changed_at_unix_ts(atom, non_neg_integer) :: success_tuple()
  def increment_source_changed_at_unix_ts(table, count) do
    add(table, @source_changed_at, count)
  end

  @spec decrement(atom) :: success_tuple()
  def decrement(table) when is_atom(table) do
    add(table, @deletes, 1)
  end

  @spec delete(atom) :: success_tuple()
  def delete(table) when is_atom(table) do
    :ets.delete(@ets_table_name, table)
    {:ok, table}
  end

  @spec get_inserts(atom) :: {:ok, non_neg_integer()}
  def get_inserts(table) do
    {:ok, counter_value(table, @inserts)}
  end

  @spec get_bq_inserts(atom) :: {:ok, non_neg_integer()}
  def get_bq_inserts(table) do
    {:ok, counter_value(table, @bq_inserts)}
  end

  # Deprecated:
  @spec log_count(Source.t() | atom) :: non_neg_integer()
  def log_count(%Source{token: token}) do
    log_count(token)
  end

  def log_count(table) when is_atom(table) do
    case lookup_counter_ref(table) do
      nil -> 0
      ref -> :atomics.get(ref, @inserts) - :atomics.get(ref, @deletes)
    end
  end

  @spec get_inserts_since_boot(atom()) :: non_neg_integer()
  def get_inserts_since_boot(table) when is_atom(table) do
    counter_value(table, @inserts_since_boot)
  end

  @spec get_total_cluster_inserts(atom()) :: non_neg_integer()
  def get_total_cluster_inserts(table) when is_atom(table) do
    counter_value(table, @total_cluster_inserts)
  end

  @spec get_source_changed_at_unix_ms(atom()) :: non_neg_integer()
  def get_source_changed_at_unix_ms(table) when is_atom(table) do
    counter_value(table, @source_changed_at)
  end

  @spec add(atom(), pos_integer(), integer()) :: success_tuple()
  defp add(table, index, count) do
    add_to_ref(table, index, count, counter_ref(table))
  end

  # Keep the increment if a source reset replaced the ref between lookup and add.
  # Exposed only to let the reset interleaving be exercised deterministically.
  @doc false
  @spec add_to_ref(atom(), pos_integer(), integer(), reference()) :: success_tuple()
  def add_to_ref(table, index, count, ref) do
    :atomics.add(ref, index, count)

    if lookup_counter_ref(table) == ref do
      {:ok, table}
    else
      add(table, index, count)
    end
  end

  @spec counter_value(atom(), pos_integer()) :: integer()
  defp counter_value(table, index) do
    case lookup_counter_ref(table) do
      nil -> 0
      ref -> :atomics.get(ref, index)
    end
  end

  @spec counter_ref(atom()) :: reference()
  defp counter_ref(table) do
    case lookup_counter_ref(table) do
      nil -> insert_counter_ref(table)
      ref -> ref
    end
  end

  @spec insert_counter_ref(atom()) :: reference()
  defp insert_counter_ref(table) do
    ref = :atomics.new(@counter_count, signed: true)

    if :ets.insert_new(@ets_table_name, {table, ref}) do
      ref
    else
      counter_ref(table)
    end
  end

  @spec lookup_counter_ref(atom()) :: reference() | nil
  defp lookup_counter_ref(table) do
    case :ets.lookup(@ets_table_name, table) do
      [{^table, ref}] -> ref
      [] -> nil
    end
  end
end
