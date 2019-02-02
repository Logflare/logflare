defmodule LogflareWeb.LogController do
  use LogflareWeb, :controller

  import Ecto.Query, only: [from: 2]

  alias Logflare.Source
  alias Logflare.Repo
  alias Logflare.User
  alias Logflare.Counter

  def create(conn, %{"log_entry" => log_entry}) do
    monotime = System.monotonic_time(:nanosecond)
    timestamp = System.system_time(:microsecond)
    unique_int = System.unique_integer([:monotonic])
    time_event = {timestamp, unique_int, monotime}
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

    message = "Logged!"

    render(conn, "index.json", message: message)
  end

  defp send_to_many_sources_by_rules(source_table, time_event, log_entry, source_name, api_key) do

    table_info =
      case Repo.get_by(Source, token: Atom.to_string(source_table)) do
        nil ->
          {:ok, table_info} = create_source(source_table, source_name, api_key)
          table_info
        table_info ->
          table_info
      end

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
        insert_log(source_table, time_event, log_entry)
      false ->
        Enum.map(rules,
          fn (x) ->
            case Regex.match?(~r{#{x.regex}}, "#{log_entry}") do
              true ->
                {:ok, sink} = Ecto.UUID.load(x.sink)
                sink_atom = String.to_atom(sink)
                insert_log(sink_atom, time_event, log_entry)
              false ->
                :ok
            end
        end)
        insert_log(source_table, time_event, log_entry)
    end
  end

  defp insert_log(source_table, time_event, log_entry) do
    case :ets.info(source_table) do
      :undefined ->
        source_table
        |> Logflare.Main.new_table()
        |> insert_and_broadcast(time_event, log_entry)
      _ ->
        insert_and_broadcast(source_table, time_event, log_entry)
    end
  end

  defp insert_and_broadcast(source_table, time_event, log_entry) do
    source_table_string = Atom.to_string(source_table)
    {timestamp, _unique_int, _monotime} = time_event
    payload = %{timestamp: timestamp, log_message: log_entry}

    :ets.insert(source_table, {time_event, payload})
    Counter.incriment(source_table)

    broadcast_log_count(source_table)
    LogflareWeb.Endpoint.broadcast("source:" <> source_table_string, "source:#{source_table_string}:new", payload)
  end

  def broadcast_log_count(source_table) do
    {:ok, log_count} = Counter.log_count(source_table)
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
