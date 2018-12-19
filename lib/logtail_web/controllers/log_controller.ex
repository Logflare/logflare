defmodule LogtailWeb.LogController do
  use LogtailWeb, :controller

  def create(conn, %{"source" => source_table, "log_entry" => log_entry}) do
    source_table = String.to_atom(source_table)
    timestamp = :os.system_time(:microsecond)
    timestamp_and_log_entry = {timestamp, log_entry}

    case :ets.info(source_table) do
      :undefined ->
        source_table
        |> Logtail.Main.new_table()
        # |> LogtailWeb.SourceController.create(conn, _params)
        |> insert_and_broadcast(timestamp_and_log_entry)

      _ ->
        insert_and_or_delete(source_table, timestamp_and_log_entry)
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
        insert_and_broadcast(source_table, timestamp_and_log_entry)
      false ->
        insert_and_broadcast(source_table, timestamp_and_log_entry)
    end
  end


  defp insert_and_broadcast(source_table, timestamp_and_log_entry) do
    source_table_string = Atom.to_string(source_table)
    {timestamp, log_entry} = timestamp_and_log_entry
    payload = %{timestamp: timestamp, log_message: log_entry}

    :ets.insert(source_table, {timestamp, payload})
    LogtailWeb.Endpoint.broadcast("source:" <> source_table_string, "source:#{source_table_string}:new", payload)
  end

end
