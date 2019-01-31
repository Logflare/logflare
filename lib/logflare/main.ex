defmodule Logflare.Main do
  use GenServer

  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def new_table(website_table) do
    GenServer.call(__MODULE__, {:create, website_table})
  end

  def delete_table(website_table) do
    GenServer.call(__MODULE__, {:stop, website_table})
    {:ok, website_table}
  end

  def delete_all_tables() do
    state = :sys.get_state(Logflare.Main)
    Enum.map(
        state, fn(t) ->
          delete_table(t)
        end
      )
    {:ok}
  end

  def update_last_updated(website_table, _last_updated) do
    {:ok, website_table}
  end

  def init(state) do
    IO.puts "Genserver Started: #{__MODULE__}"
    {:ok, state}
  end

  def handle_call({:create, website_table}, _from, state) do
    Logflare.TableOwner.start_link(website_table)
    state = [website_table | state]
    # table_count = Enum.count(state)
    # IO.inspect(table_count, label: "Table count")
    {:reply, website_table, state}
  end

  def handle_call({:stop, website_table}, _from, state) do
    GenServer.stop(website_table)
    state = List.delete(state, website_table)
    # table_count = Enum.count(state)
    # IO.inspect(table_count, label: "Table count")
    {:reply, website_table, state}
  end

  def handle_info({:last_updated, _website_table}, state) do
    {:noreply, state}
  end

end
