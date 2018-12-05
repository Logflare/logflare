defmodule Logtail.Main do
  use GenServer

  def start_link do
    GenServer.start_link(__MODULE__, %{}, [])
  end

  def init(args) do
    IO.puts "Genserver Started: #{__MODULE__}"
    {:ok, args}
  end
#
#  def handle_cast(:insert_log, ) do
#    insert_log(website_table, timestamp, log_entry)
#    {:noreply, website_table}
#  end
#
#  def handle_call(:puts, message) do
#    {:reply, message}
#  end
#
#  def insert_log(website_table, timestamp, log_entry) do
#    case :ets.info(website_table) do
#      :undefined ->
#        website_table
#        |> :ets.new([:named_table, :ordered_set, :public])
#        |> :ets.insert({timestamp, log_entry})
#      _ ->
#        :ets.insert(website_table, {timestamp, log_entry})
#    end
#  end

end
