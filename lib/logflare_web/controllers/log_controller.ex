defmodule LogflareWeb.LogController do
  use LogflareWeb, :controller

  import Ecto.Query, only: [from: 2]

  alias Logflare.Source
  alias Logflare.Repo
  alias Logflare.User

  def create(conn, %{"log_entry" => log_entry}) do
    monotime = System.monotonic_time(:nanosecond)
    timestamp = System.system_time(:microsecond)
    unique_int = System.unique_integer([:monotonic])
    time_event = {monotime, timestamp, unique_int}
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

    send_to_many_sources_by_rules(source_table, time_event, log_entry, source_name, api_key)
    # create_table_maybe_and_insert(source_table, time_event, log_entry, source_name, api_key)

    message = "Logged!"

    render(conn, "index.json", message: message)
  end

  defp create_table_maybe_and_insert(source_table, time_event, log_entry, source_name, api_key) do
    case :ets.info(source_table) do
      :undefined ->
        source_table
        |> Logflare.Main.new_table()
        |> insert_and_broadcast(time_event, log_entry)

        create_source(source_table, source_name, api_key)
      _ ->
        insert_and_or_delete(source_table, time_event, log_entry)
    end
  end

  defp send_to_many_sources_by_rules(source_table, time_event, log_entry, source_name, api_key) do
    #{:ok, source_uuid} = Ecto.UUID.dump(Atom.to_string(source_table))
    table_info = Repo.get_by(Source, token: Atom.to_string(source_table))

    rules_query = from r in "rules",
      where: r.source_id == ^table_info.id,
      select: %{
        id: r.id,
        regex: r.regex,
        sink: r.sink,
        sink_id: r.source_id,
      }

    rules = Repo.all(rules_query)

    case rules == [] do
      true ->
        create_table_maybe_and_insert(source_table, time_event, log_entry, source_name, api_key)
      false ->
        Enum.map(rules,
          fn (x) ->
            case Regex.match?(~r{#{x.regex}}, log_entry) do
              true ->
                {:ok, sink} = Ecto.UUID.load(x.sink)
                sink_atom = String.to_atom(sink)
                sink_name = Repo.get(Source, table_info.id).name
                create_table_maybe_and_insert(sink_atom, time_event, log_entry, sink_name, api_key)
                create_table_maybe_and_insert(source_table, time_event, log_entry, source_name, api_key)
              false ->
                create_table_maybe_and_insert(source_table, time_event, log_entry, source_name, api_key)
            end
        end)
    end
  end

  defp insert_and_or_delete(source_table, time_event, log_entry) do
    log_count = :ets.info(source_table)

    case log_count[:size] >= 3000 do
      true ->
        first_log = :ets.first(source_table)
        :ets.delete(source_table, first_log)
        insert_and_broadcast(source_table, time_event, log_entry)
      false ->
        insert_and_broadcast(source_table, time_event, log_entry)
    end
  end

  defp insert_and_broadcast(source_table, time_event, log_entry) do
    source_table_string = Atom.to_string(source_table)
    {_monotime, timestamp, _unique_int} = time_event
    payload = %{timestamp: timestamp, log_message: log_entry}

    :ets.insert(source_table, {time_event, payload})

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
