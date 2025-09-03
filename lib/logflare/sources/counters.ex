defmodule Logflare.Sources.Counters do
  @moduledoc false
  @callback get_inserts(atom) :: {:ok, integer}
  alias Logflare.Sources.Source
  use GenServer

  require Logger

  @ets_table_name :table_counters
  @type success_tuple :: {:ok, atom}

  def start_link(args \\ []) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def init(state) do
    Process.flag(:trap_exit, true)

    :ets.new(@ets_table_name, [:public, :named_table])
    {:ok, state}
  end

  def terminate(reason, _state) do
    Logger.warning("[#{__MODULE__}] terminating - #{reason} ")
    reason
  end

  @spec create(atom) :: success_tuple
  def create(table) do
    default = make_default(table)
    :ets.update_counter(@ets_table_name, table, {2, 0}, default)
    :ets.update_counter(@ets_table_name, table, {3, 0}, default)
    :ets.update_counter(@ets_table_name, table, {4, 0}, default)
    :ets.update_counter(@ets_table_name, table, {5, 0}, default)
    :ets.update_counter(@ets_table_name, table, {6, 0}, default)
    {:ok, table}
  end

  @spec increment(atom) :: success_tuple
  @spec increment(atom, non_neg_integer()) :: success_tuple
  def increment(table, n \\ 1) do
    :ets.update_counter(@ets_table_name, table, {2, n}, make_default(table))
    {:ok, table}
  end

  @spec increment_bq_count(atom, non_neg_integer) :: success_tuple
  def increment_bq_count(table, count) do
    :ets.update_counter(@ets_table_name, table, {4, count}, make_default(table))
    {:ok, table}
  end

  @spec increment_inserts_since_boot_count(atom, non_neg_integer) :: success_tuple
  def increment_inserts_since_boot_count(table, count) do
    :ets.update_counter(@ets_table_name, table, {5, count}, make_default(table))
    {:ok, table}
  end

  @spec increment_total_cluster_inserts_count(atom, non_neg_integer) :: success_tuple
  def increment_total_cluster_inserts_count(table, count) do
    :ets.update_counter(@ets_table_name, table, {6, count}, make_default(table))
    {:ok, table}
  end

  @spec increment_source_changed_at_unix_ts(atom, non_neg_integer) :: success_tuple
  def increment_source_changed_at_unix_ts(table, count) do
    :ets.update_counter(@ets_table_name, table, {7, count}, make_default(table))
    {:ok, table}
  end

  @spec decrement(atom) :: success_tuple
  def decrement(table) when is_atom(table) do
    :ets.update_counter(@ets_table_name, table, {3, 1}, make_default(table))
    {:ok, table}
  end

  @spec delete(atom) :: success_tuple
  def delete(table) when is_atom(table) do
    :ets.delete(@ets_table_name, table)
    {:ok, table}
  end

  @spec get_inserts(atom) :: {:ok, non_neg_integer}
  def get_inserts(table) do
    case :ets.lookup(@ets_table_name, table) do
      [
        {_table, inserts, _deletes, _total_inserts_in_bq, _inserts_since_boot,
         _total_cluster_inserts, _changed_at}
      ] ->
        {:ok, inserts}

      _ ->
        {:ok, 0}
    end
  end

  @spec get_bq_inserts(atom) :: {:ok, non_neg_integer}
  def get_bq_inserts(table) do
    case :ets.lookup(@ets_table_name, table) do
      [
        {_table, _inserts, _deletes, total_inserts_in_bq, _inserts_since_boot,
         _total_cluster_inserts, _changed_at}
      ] ->
        {:ok, total_inserts_in_bq}

      _ ->
        {:ok, 0}
    end
  end

  # Deprecated: should be the count of things in the RecentEventsTouch ets table but never could get things
  # incrementing / decrementing correctly.
  @spec log_count(Source.t() | atom) :: non_neg_integer
  def log_count(%Source{token: token}) do
    log_count(token)
  end

  def log_count(table) when is_atom(table) do
    case :ets.lookup(@ets_table_name, table) do
      [
        {_table, inserts, deletes, _total_inserts_in_bq, _inserts_since_boot,
         _total_cluster_inserts, _changed_at}
      ] ->
        count = inserts - deletes
        count

      _ ->
        0
    end
  end

  def get_inserts_since_boot(table) when is_atom(table) do
    case :ets.lookup(@ets_table_name, table) do
      [
        {_table, _inserts, _deletes, _total_inserts_in_bq, inserts_since_boot,
         _total_cluster_inserts, _changed_at}
      ] ->
        inserts_since_boot

      _ ->
        0
    end
  end

  def get_total_cluster_inserts(table) when is_atom(table) do
    case :ets.lookup(@ets_table_name, table) do
      [
        {_table, _inserts, _deletes, _total_inserts_in_bq, _inserts_since_boot,
         total_cluster_inserts, _changed_at}
      ] ->
        total_cluster_inserts

      _ ->
        0
    end
  end

  def get_source_changed_at_unix_ms(table) when is_atom(table) do
    case :ets.lookup(@ets_table_name, table) do
      [
        {_table, _inserts, _deletes, _total_inserts_in_bq, _inserts_since_boot,
         _total_cluster_inserts, changed_at}
      ] ->
        changed_at

      _ ->
        0
    end
  end

  defp make_default(table), do: {table, 0, 0, 0, 0, 0, 0}
end
