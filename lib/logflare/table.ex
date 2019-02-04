defmodule Logflare.Table do
  use GenServer

  alias Logflare.Counter
  alias LogflareWeb.LogController

  @ttl 2592000000000 # one month
  @ttl_timer 1000
  @prune_timer 1000

  def start_link(website_table) do
    GenServer.start_link(__MODULE__, website_table, name: website_table)
  end

  ## Client

  def init(state) do
    IO.puts "Genserver Started: #{state}"

    tab_path = "tables/" <> Atom.to_string(state) <> ".tab"

    case :ets.tabfile_info(String.to_charlist(tab_path)) do
      {:ok, info} ->
        IO.puts("Loaded table!")
        :ets.file2tab(String.to_charlist(tab_path))
        log_count = info[:size]
        Counter.create(state)
        Counter.incriment(state, log_count)
      {:error, _reason} ->
        IO.puts("Created table!")
        table_args = [:named_table, :ordered_set, :public]
        :ets.new(state, table_args)
        Counter.create(state)
    end
    check_ttl()
    prune()

    state = [{:table, state}]
    {:ok, state}
  end

  ## Server

  def handle_info(:ttl, state) do
    website_table = state[:table]
    first = :ets.first(website_table)

    case first != :"$end_of_table" do
      true ->
        {timestamp, _unique_int, _monotime} = first
        now = System.os_time(:microsecond)
        day_ago = now - @ttl
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
        for _log <- 1001..count do
          log = :ets.first(website_table)
          :ets.delete(website_table, log)
          Counter.decriment(website_table)
          LogController.broadcast_log_count(website_table)
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
    Process.send_after(self(), :ttl, @ttl_timer)
  end

  defp prune() do
    Process.send_after(self(), :prune, @prune_timer)
  end

end
