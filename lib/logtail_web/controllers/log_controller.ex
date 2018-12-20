defmodule LogtailWeb.LogController do
  use LogtailWeb, :controller

  alias Logtail.Source
  alias Logtail.Repo
  alias Logtail.User

  def create(conn, %{"log_entry" => log_entry}) do
    timestamp = :os.system_time(:microsecond)
    timestamp_and_log_entry = {timestamp, log_entry}
    api_key = Enum.into(conn.req_headers, %{})["x-api-key"]
    source_name = conn.params["source_name"]

    source_table =
      case conn.params["source"] == nil do
        true ->
          String.to_atom(Ecto.UUID.generate())
        false ->
          String.to_atom(conn.params["source"])
      end

    IO.puts("++++++")
    IO.inspect(source_table)

    case :ets.info(source_table) do
      :undefined ->
        source_table
        |> Logtail.Main.new_table()
        |> insert_and_broadcast(timestamp_and_log_entry)

        create_source(source_table, source_name, api_key)
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

  def create_source(source_table, source_name, api_key) do
    source_table_string = Atom.to_string(source_table)

    IO.puts("++++++")
    IO.inspect(source_table_string)

    source = %{token: source_table_string, name: source_name}

    changeset = Repo.get_by(User, api_key: api_key)
      |> Ecto.build_assoc(:sources)
      |> Source.changeset(source)
      |> Repo.insert()

    IO.puts("++++++")
    IO.inspect(changeset)
  end

end
