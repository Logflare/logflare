defmodule Logflare.Table do
  use GenServer

  alias Logflare.Counter
  alias LogflareWeb.LogController

  def start_link(website_table) do
    GenServer.start_link(__MODULE__, website_table, name: website_table)
  end

  ## Client

  def init(state) do
    GenServer.cast(state, {:create, state})
    IO.puts "Genserver Started: #{state}"
    prune()
    check_ttl()
    # need to put TTL back here
    {:ok, state}
  end

  ## Server

  def handle_cast({:create, website_table}, state) do
    table_args = [:named_table, :ordered_set, :public]
    :ets.new(website_table, table_args)
    state = [{:table, website_table}]
    check_ttl()
    {:noreply, state}
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
          Counter.decriment(website_table)
          LogController.broadcast_log_count(website_table)
        end
        check_ttl()
        {:noreply, state}
      false ->
        check_ttl()
        {:noreply, state}
    end
  end

  def handle_info(:prune, state) do
    website_table = state[:table]
    {:ok, count} = Counter.log_count(website_table)

    case count > 1000 do
      true ->
        for log <- 1001..count do
          Task.Supervisor.start_child(Logflare.TaskSupervisor, fn ->
            log = :ets.first(website_table)
            :ets.delete(website_table, log)
            Counter.decriment(website_table)
            LogController.broadcast_log_count(website_table)
          end)
        end
        prune()
        {:noreply, state}
      false ->
        prune()
        {:noreply, state}
    end
  end

  ## Private Functions

  defp check_ttl() do
    Process.send_after(self(), :ttl, 1000)
  end

  defp prune() do
    Process.send_after(self(), :prune, 1000)
  end

end
