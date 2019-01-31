defmodule Logflare.TableOwner do
  use GenServer

  def start_link(website_table) do
    GenServer.start_link(__MODULE__, [], name: website_table)
    GenServer.call(website_table, {:create, website_table})
  end

#  def new_table(website_table) do
#    GenServer.call(__MODULE__, {:create, website_table})
#  end

  def init(state) do
    IO.puts "Genserver Started: #{__MODULE__}"
    {:ok, state}
  end

  def handle_call({:create, website_table}, _from, state) do
    table = website_table
    table_args = [:named_table, :ordered_set, :public]
    :ets.new(table, table_args)
    {:reply, website_table, state}
  end

end
