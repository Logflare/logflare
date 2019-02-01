defmodule Logflare.Counter do
  use GenServer

  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(state) do
    IO.puts "Genserver Started: #{__MODULE__}"
    :ets.new(:counters, [:public, :named_table])
    {:ok, state}
  end

  def incriment(table) do
    :ets.update_counter(:counters, table, {2, 1}, {table, 0, 0})
    {:ok, table}
  end

  def decriment(table) do
    :ets.update_counter(:counters, table, {3, 1}, {table, 0, 0})
    {:ok, table}
  end

  def log_count(table) do
    [{_table, inserts, deletes}] = :ets.lookup(:counters, table)
    count = inserts - deletes
    {:ok, count}
  end

end
