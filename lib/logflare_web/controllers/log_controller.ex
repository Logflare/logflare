defmodule LogflareWeb.LogController do
  use LogflareWeb, :controller

  alias Logflare.{Source, Sources}
  alias Logflare.Repo
  alias Logflare.User
  alias Logflare.TableCounter
  alias Logflare.SystemCounter
  alias Logflare.TableManager
  alias Logflare.SourceData
  alias Logflare.AccountCache
  alias Logflare.TableBuffer
  alias Logflare.Logs

  @system_counter :total_logs_logged

  def create(conn, %{"log_entry" => log_entry}) do
    monotime = System.monotonic_time(:nanosecond)
    timestamp = System.system_time(:microsecond)
    unique_int = System.unique_integer([:monotonic])
    time_event = {timestamp, unique_int, monotime}
    api_key = Enum.into(conn.req_headers, %{})["x-api-key"]

    metadata =
      case is_map(conn.params["metadata"]) do
        false ->
          nil

        true ->
          conn.params["metadata"]
      end

    source_id =
      case conn.params["source"] == nil do
        true ->
          source_name = conn.params["source_name"]

          lookup_or_create_source(api_key, source_name)

        false ->
          String.to_atom(conn.params["source"])
      end

    source = Sources.Cache.get_by_id(source_id)

    if is_nil(source.overflow_source) == false do
      if source_over_threshold?(source_id) do
        send_to_many_sources_by_rules(
          String.to_atom(source.overflow_source),
          time_event,
          log_entry,
          metadata,
          api_key
        )
      end
    end

    send_to_many_sources_by_rules(source_id, time_event, log_entry, metadata, api_key)

    message = "Logged!"

    render(conn, "index.json", message: message)
  end

  defp send_to_many_sources_by_rules(source_id, time_event, log_entry, metadata, _api_key) do
    rules = Sources.Cache.get_by_id(source_id).rules

    case rules == [] do
      true ->
        insert_log(source_id, time_event, log_entry, metadata)

      false ->
        Enum.map(
          rules,
          fn x ->
            case Regex.match?(~r{#{x.regex}}, "#{log_entry}") do
              true ->
                sink_atom = String.to_atom(x.sink)
                insert_log(sink_atom, time_event, log_entry, metadata)

              false ->
                :ok
            end
          end
        )

        insert_log(source_id, time_event, log_entry, metadata)
    end
  end

  defp insert_log(source_table, time_event, log_entry, metadata) do
    case :ets.info(source_table) do
      :undefined ->
        source_table
        |> TableManager.new_table()
        |> insert_and_broadcast(time_event, log_entry, metadata)

      _ ->
        insert_and_broadcast(source_table, time_event, log_entry, metadata)
    end
  end

  defp insert_and_broadcast(source_table, time_event, log_entry, metadata) do
    source_table_string = Atom.to_string(source_table)
    {timestamp, _unique_int, _monotime} = time_event

    payload =
      case is_nil(metadata) do
        true ->
          %{timestamp: timestamp, log_message: log_entry}

        false ->
          %{timestamp: timestamp, log_message: log_entry, metadata: metadata}
      end

    Logs.insert_or_push(source_table, {time_event, payload})

    TableBuffer.push(source_table_string, {time_event, payload})
    TableCounter.incriment(source_table)
    SystemCounter.incriment(@system_counter)

    Logs.broadcast_log_count(source_table)
    Logs.broadcast_total_log_count()

    LogflareWeb.Endpoint.broadcast(
      "source:" <> source_table_string,
      "source:#{source_table_string}:new",
      payload
    )
  end

  defp create_source(source_name, api_key) do
    source = %{token: Ecto.UUID.generate(), name: source_name}

    Repo.get_by(User, api_key: api_key)
    |> Ecto.build_assoc(:sources)
    |> Source.changeset(source)
    |> Repo.insert()
  end

  defp lookup_or_create_source(api_key, source_name) do
    source = Sources.Cache.get_by_name(source_name)

    case source do
      nil ->
        {:ok, new_source} = create_source(source_name, api_key)
        new_source.token

      _ ->
        source.token
    end
  end

  defp source_over_threshold?(source) do
    current_rate = SourceData.get_rate(source)
    avg_rate = SourceData.get_avg_rate(source)

    case avg_rate >= 1 do
      true ->
        current_rate / 10 >= avg_rate

      false ->
        false
    end
  end
end
