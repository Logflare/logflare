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
    check_ttl(5000)
    {:reply, website_table, state}
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
          # Match an argument list of three, where the second argument is a number > 3:
          # [{['_', '$1', '_'],
          #   [{ '>', '$1', 3}],
          #     []}]
          :ets.delete(website_table, first)
          IO.puts("deleted stuff")
        end
        check_ttl(100)
        # GenServer.cast(self(), :ttl) # loop
        {:noreply, state}
      false ->
        check_ttl(5000) # Reschedule once more
        {:noreply, state}
    end
  end

  defp check_ttl(ms) do
    Process.send_after(self(), :ttl, ms)
  end

end
