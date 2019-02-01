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
    :ets.update_counter(:counters, table, 1, {1, 0})
    {:ok, table}
  end

  def decriment(table) do
    :ets.update_counter(:counters, table, -1, {1, 0})
    {:ok, table}
  end

  def get(table) do
    count = GenServer.call(table, :count)
    {:ok, count}
  end
end
