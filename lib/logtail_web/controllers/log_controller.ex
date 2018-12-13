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
        :ets.insert(source_table, {timestamp, log_entry})
      end
    message = "Logged!"

    render(conn, "index.json", message: message)
  end

end
