defmodule Logflare.TableCounter do
  use GenServer

  require Logger

  @ets_table_name :table_counters

  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(state) do
    Logger.info("Table counters started!")
    :ets.new(@ets_table_name, [:public, :named_table])
    {:ok, state}
  end

  @spec create(:atom) :: {}
  def create(table) do
    :ets.update_counter(@ets_table_name, table, {2, 0}, {table, 0, 0, 0})
    :ets.update_counter(@ets_table_name, table, {3, 0}, {table, 0, 0, 0})
    :ets.update_counter(@ets_table_name, table, {4, 0}, {table, 0, 0, 0})
    {:ok, table}
  end

  @spec incriment(:atom) :: {}
  def incriment(table) do
    :ets.update_counter(@ets_table_name, table, {2, 1}, {table, 0, 0, 0})
    :ets.update_counter(@ets_table_name, table, {4, 1}, {table, 0, 0, 0})
    {:ok, table}
  end

  @spec incriment_ets_count(:atom, INTEGER) :: {}
  def incriment_ets_count(table, count) do
    :ets.update_counter(@ets_table_name, table, {2, count}, {table, 0, 0, 0})
    {:ok, table}
  end

  @spec incriment_total_count(:atom, INTEGER) :: {}
  def incriment_total_count(table, count) do
    :ets.update_counter(@ets_table_name, table, {4, count}, {table, 0, 0, 0})
    {:ok, table}
  end

  @spec decriment(:atom) :: {}
  def decriment(table) do
    :ets.update_counter(@ets_table_name, table, {3, 1}, {table, 0, 0, 0})
    {:ok, table}
  end

  @spec delete(:atom) :: {}
  def delete(table) do
    :ets.delete(@ets_table_name, table)
    {:ok, table}
  end

  @spec get_inserts(:atom) :: {}
  def get_inserts(table) do
    case :ets.lookup(@ets_table_name, table) do
      [{_table, inserts, _deletes, _total_inserts}] ->
        {:ok, inserts}

      _ ->
        {:ok, 0}
    end
  end

  @spec get_total_inserts(:atom) :: {}
  def get_total_inserts(table) do
    case :ets.lookup(@ets_table_name, table) do
      [{_table, _inserts, _deletes, total_inserts}] ->
        {:ok, total_inserts}

      _ ->
        {:ok, 0}
    end
  end

  @spec log_count(:atom) :: {}
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
