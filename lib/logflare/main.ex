defmodule Logflare.Main do
  use GenServer

  def start_link do
    GenServer.start_link(__MODULE__, 0, name: __MODULE__)
  end

  def new_table(website_table) do
    GenServer.call(__MODULE__, {:create, website_table})
  end

  def init(state) do
    IO.puts "Genserver Started: #{__MODULE__}"
    {:ok, state}
  end

  def handle_call({:create, website_table}, _from, state) do
    table = website_table
    table_args = [:named_table, :ordered_set, :public]
    :ets.new(table, table_args)
    IO.inspect({:reply, table, state+1}, label: "New table!")
  end

end
