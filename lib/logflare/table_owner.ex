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
    state = [{:table, website_table}]
    {:reply, website_table, state}
    check_ttl()
  end

  def handle_info(:ttl, state) do
    website_table = state[:table]
    first = :ets.first(website_table)

    case first != :"$end_of_table" do
      true ->
        {_monotime, timestamp, _unique_int} = first
        now = System.os_time(:microsecond)
        day_ago = now - 86400000000
        if timestamp < day_ago do
          # :ets.delete_match(website_table) I'm too dumb for this
          # https://github.com/ericmj/ex2ms

          :ets.delete(website_table, first)
          IO.puts("deleted stuff")
        end
        check_ttl()
        # GenServer.cast(self(), :ttl) # loop
        {:noreply, state}
      false ->
        check_ttl() # Reschedule once more
        {:noreply, state}
    end
  end

  defp check_ttl() do
    Process.send_after(self(), :ttl, 1000)
  end

end
