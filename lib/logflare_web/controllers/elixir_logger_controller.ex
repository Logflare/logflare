defmodule LogflareWeb.ElixirLoggerController do
  use LogflareWeb, :controller

  alias Logflare.Source
  alias Logflare.Repo
  alias Logflare.User
  alias Logflare.TableCounter
  alias Logflare.SystemCounter
  alias Logflare.TableManager
  alias Logflare.SourceData
  alias Logflare.AccountCache
  alias Logflare.TableBuffer

  @system_counter :total_logs_logged

  def create(conn, %{"batch" => batch, "source_name" => source_name}) do
    api_key = Enum.into(conn.req_headers, %{})["x-api-key"]

    message = "Logged!"

    for log_entry <- batch do
      process_log(log_entry, %{source: source_name, api_key: api_key})
    end

    render(conn, "index.json", message: message)
  end

  def process_log(log_entry, %{api_key: api_key, source: source_name}) do
    %{"message" => m, "metadata" => metadata, "timestamp" => ts, "level" => lv} = log_entry
    monotime = System.monotonic_time(:nanosecond)
    datetime = Timex.parse!(ts, "{ISO:Extended}")
    timestamp = Timex.to_unix(datetime) * 1_000_000
    unique_int = System.unique_integer([:monotonic])
    time_event = {timestamp, unique_int, monotime}

    metadata = metadata |> Map.put("level", lv)

    source_table =
      api_key
      |> lookup_or_create_source(source_name)
      |> String.to_atom()

    %{overflow_source: overflow_source} =
      AccountCache.get_source(api_key, Atom.to_string(source_table))

    send_with_data = &send_to_many_sources_by_rules(&1, time_event, m, metadata, api_key)

    if overflow_source && source_over_threshold?(source_table) do
      overflow_source
      |> String.to_atom()
      |> send_with_data.()
    end

    send_with_data.(source_table)
  end

  def broadcast_log_count(source_table) do
    {:ok, log_count} = TableCounter.get_total_inserts(source_table)
    source_table_string = Atom.to_string(source_table)
    payload = %{log_count: log_count, source_token: source_table_string}

    LogflareWeb.Endpoint.broadcast(
      "dashboard:" <> source_table_string,
      "dashboard:#{source_table_string}:log_count",
      payload
    )
  end

  def broadcast_total_log_count() do
    {:ok, log_count} = SystemCounter.log_count(@system_counter)
    payload = %{total_logs_logged: log_count}

    LogflareWeb.Endpoint.broadcast("everyone", "everyone:update", payload)
  end

  defp send_to_many_sources_by_rules(source_table, time_event, log_entry, metadata, api_key) do
    rules = AccountCache.get_rules(api_key, Atom.to_string(source_table))

    Enum.each(
      rules,
      fn x ->
        if Regex.match?(~r{#{x.regex}}, "#{log_entry}") do
          x.sink
          |> String.to_atom()
          |> insert_log(time_event, log_entry, metadata)
        end
      end
    )

    insert_log(source_table, time_event, log_entry, metadata)
  end

  defp insert_log(source_table, time_event, log_entry, metadata) do
    source_table =
      if :ets.info(source_table) == :undefined do
        TableManager.new_table(source_table)
      else
        source_table
      end

    insert_and_broadcast(source_table, time_event, log_entry, metadata)
  end

  defp insert_and_broadcast(source_table, time_event, log_entry, metadata) do
    source_table_string = Atom.to_string(source_table)
    {timestamp, _unique_int, _monotime} = time_event

    payload =
      if metadata do
        %{timestamp: timestamp, log_message: log_entry, metadata: metadata}
      else
        %{timestamp: timestamp, log_message: log_entry}
      end

    :ets.insert(source_table, {time_event, payload})
    TableBuffer.push(source_table_string, {time_event, payload})
    TableCounter.incriment(source_table)
    SystemCounter.incriment(@system_counter)

    broadcast_log_count(source_table)
    broadcast_total_log_count()

    LogflareWeb.Endpoint.broadcast(
      "source:" <> source_table_string,
      "source:#{source_table_string}:new",
      payload
    )
  end

  defp create_source(source_name, api_key) do
    source = %{token: Ecto.UUID.generate(), name: source_name}

    User
    |> Repo.get_by(api_key: api_key)
    |> Ecto.build_assoc(:sources)
    |> Source.changeset(source)
    |> Repo.insert()
  end

  defp lookup_or_create_source(api_key, source_name) do
    source = AccountCache.get_source_by_name(api_key, source_name)

    source =
      if source do
        source
      else
        {:ok, new_source} = create_source(source_name, api_key)
        AccountCache.update_account(api_key)

        new_source
      end

    source.token
  end

  defp source_over_threshold?(source) do
    current_rate = SourceData.get_rate(source)
    avg_rate = SourceData.get_avg_rate(source)

    avg_rate >= 1 and current_rate / 10 >= avg_rate
  end
end
