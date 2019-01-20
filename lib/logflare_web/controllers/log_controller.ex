defmodule LogflareWeb.LogController do
  use LogflareWeb, :controller

  alias Logflare.Source
  alias Logflare.Repo
  alias Logflare.User

  def create(conn, %{"log_entry" => log_entry}) do
    timestamp = :os.system_time(:microsecond)
    timestamp_and_log_entry = {timestamp, log_entry}
    api_key = Enum.into(conn.req_headers, %{})["x-api-key"]

    source_table =
      case conn.params["source"] == nil do
        true ->
          source_name = conn.params["source_name"]
          lookup_or_gen_source_token(source_name)
          |> String.to_atom()
        false ->
          String.to_atom(conn.params["source"])
      end

    source_name =
      case conn.params["source_name"] == nil do
        true ->
          source_table_string = Atom.to_string(source_table)
          Repo.get_by(Source, token: source_table_string)
        false ->
          conn.params["source_name"]
      end

    case :ets.info(source_table) do
      :undefined ->
        source_table
        |> Logflare.Main.new_table()
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

    case log_count[:size] >= 3000 do
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

    log_count = :ets.info(source_table)[:size]

    broadcast_log_count(source_table, log_count)
    LogflareWeb.Endpoint.broadcast("source:" <> source_table_string, "source:#{source_table_string}:new", payload)
  end

  defp broadcast_log_count(source_table, log_count) do
    source_table_string = Atom.to_string(source_table)

    payload = %{log_count: log_count, source_token: source_table_string}

    LogflareWeb.Endpoint.broadcast("dashboard", "dashboard:update", payload)
  end

  defp create_source(source_table, source_name, api_key) do
    source_table_string = Atom.to_string(source_table)

    source = %{token: source_table_string, name: source_name}

    Repo.get_by(User, api_key: api_key)
      |> Ecto.build_assoc(:sources)
      |> Source.changeset(source)
      |> Repo.insert()
  end

  defp lookup_or_gen_source_token(source_name) do
    source = Repo.get_by(Source, name: source_name)

    case source do
      nil ->
        Ecto.UUID.generate()
      _ ->
        source.token
    end
  end

end
