defmodule LogtailWeb.LogController do
  use LogtailWeb, :controller

#  use Logtail.Main
#
#  def create(conn, %{"id" => source_table}) do
#    GenServer.cast(Logtail.Main, )
#  end

  def create(conn, %{"source" => source_table, "log_entry" => log_entry}) do
    source_table = String.to_atom(source_table)
    timestamp = Integer.to_string(:os.system_time(:millisecond))
    # {ts, _} = Integer.parse(timestamp)
    case :ets.info(source_table) do
      :undefined ->
        source_table
        |> Logtail.Main.new_table()
        |> :ets.insert({timestamp, log_entry})
      _ ->
        insert_and_or_delete(source_table, {timestamp, log_entry})
      end
    message = "Logged!"

    render(conn, "index.json", message: message)
  end

  defp insert_and_or_delete(source_table, timestamp_and_log_entry) do
    log_count = :ets.info(source_table)

    case log_count[:size] >= 100 do
      true ->
        first_log = :ets.first(source_table)
        :ets.delete(source_table, first_log)
        IO.puts("+++DELETED STUFF+++")
        :ets.insert(source_table, timestamp_and_log_entry)
        IO.puts("+++INSERTED STUFF+++")
      false ->
        :ets.insert(source_table, timestamp_and_log_entry)
        IO.puts("+++INSERTED STUFF+++")
    end
  end


end
