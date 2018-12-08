defmodule LogtailWeb.LogController do
  use LogtailWeb, :controller

#  use Logtail.Main
#
#  def create(conn, %{"id" => website_table}) do
#    GenServer.cast(Logtail.Main, )
#  end

  def create(conn, %{"website_table" => website_table, "timestamp" => timestamp, "log_entry" => log_entry}) do
    website_table = String.to_atom(website_table)
    # {ts, _} = Integer.parse(timestamp)
    case :ets.info(website_table) do
      :undefined ->
        website_table
        |> Logtail.Main.new_table()
        |> :ets.insert({timestamp, log_entry})
        _ ->
        :ets.insert(website_table, {timestamp, log_entry})
      end
    message = "Logged!"

    render(conn, "index.json", message: message)
  end

  def show(conn, %{"id" => id}) do
    table_id = String.to_atom(id)
    logs = :ets.match(table_id, {:"$0", :"$1"})
    render(conn, "show.html", logs: logs)
  end

end
