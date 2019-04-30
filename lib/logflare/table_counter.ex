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

  def create(table) do
    :ets.update_counter(@ets_table_name, table, {2, 0}, {table, 0, 0})
    :ets.update_counter(@ets_table_name, table, {3, 0}, {table, 0, 0})
    {:ok, table}
  end

  def incriment(table) do
    :ets.update_counter(@ets_table_name, table, {2, 1}, {table, 0, 0})
    {:ok, table}
  end

  def incriment(table, count) do
    :ets.update_counter(@ets_table_name, table, {2, count}, {table, 0, 0})
    {:ok, table}
  end

  def decriment(table) do
    :ets.update_counter(@ets_table_name, table, {3, 1}, {table, 0, 0})
    {:ok, table}
  end

  def delete(table) do
    :ets.delete(@ets_table_name, table)
    {:ok, table}
  end

  def get_inserts(table) do
    [{_table, inserts, _deletes}] = :ets.lookup(@ets_table_name, table)
    {:ok, inserts}
  end

  def log_count(table) do
    [{_table, inserts, deletes}] = :ets.lookup(@ets_table_name, table)
    count = inserts - deletes
    {:ok, count}
  end
end
