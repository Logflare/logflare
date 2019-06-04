defmodule Logflare.Sources.Counters do
  @callback get_inserts(atom) :: {:ok, integer}
  use GenServer

  require Logger

  @ets_table_name :table_counters
  @type success_tuple :: {:ok, atom}

  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(state) do
    Logger.info("Table counters started!")
    :ets.new(@ets_table_name, [:public, :named_table])
    {:ok, state}
  end

  @spec create(atom) :: success_tuple
  def create(table) do
    :ets.update_counter(@ets_table_name, table, {2, 0}, {table, 0, 0, 0})
    :ets.update_counter(@ets_table_name, table, {3, 0}, {table, 0, 0, 0})
    :ets.update_counter(@ets_table_name, table, {4, 0}, {table, 0, 0, 0})
    {:ok, table}
  end

  @spec incriment(atom) :: success_tuple
  def incriment(table) do
    :ets.update_counter(@ets_table_name, table, {2, 1}, {table, 0, 0, 0})
    :ets.update_counter(@ets_table_name, table, {4, 1}, {table, 0, 0, 0})
    {:ok, table}
  end

  @spec incriment_ets_count(atom, non_neg_integer) :: success_tuple
  def incriment_ets_count(table, count) do
    :ets.update_counter(@ets_table_name, table, {2, count}, {table, 0, 0, 0})
    {:ok, table}
  end

  @spec incriment_total_count(atom, non_neg_integer) :: success_tuple
  def incriment_total_count(table, count) do
    :ets.update_counter(@ets_table_name, table, {4, count}, {table, 0, 0, 0})
    {:ok, table}
  end

  @spec decriment(atom) :: success_tuple
  def decriment(table) do
    :ets.update_counter(@ets_table_name, table, {3, 1}, {table, 0, 0, 0})
    {:ok, table}
  end

  @spec delete(atom) :: success_tuple
  def delete(table) do
    :ets.delete(@ets_table_name, table)
    {:ok, table}
  end

  @spec get_inserts(atom) :: {:ok, non_neg_integer}
  def get_inserts(table) do
    case :ets.lookup(@ets_table_name, table) do
      [{_table, inserts, _deletes, _total_inserts}] ->
        {:ok, inserts}

      _ ->
        {:ok, 0}
    end
  end

  @spec get_total_inserts(atom) :: {:ok, non_neg_integer}
  def get_total_inserts(table) do
    case :ets.lookup(@ets_table_name, table) do
      [{_table, _inserts, _deletes, total_inserts}] ->
        {:ok, total_inserts}

      _ ->
        {:ok, 0}
    end
  end

  @spec log_count(atom) :: {:ok, non_neg_integer}
  def log_count(table) do
    case :ets.lookup(@ets_table_name, table) do
      [{_table, inserts, deletes, _total_inserts}] ->
        count = inserts - deletes
        {:ok, count}

      _ ->
        {:ok, 0}
    end
  end
end
