defmodule LogflareWeb.ElixirLoggerController do
  use LogflareWeb, :controller

  alias Logflare.Google.BigQuery
  alias Logflare.{SourceCounter, SystemCounter, Sources, Source, SourceData, SourceBuffer, Logs}
  alias Logflare.Logs.Injest

  @system_counter :total_logs_logged

  def create(conn, %{"batch" => batch} = params) do
    message = "Logged!"

    if BigQuery.Validator.NestedValues.valid?(params) do
      for log_entry <- batch do
        process_log(log_entry, conn.assigns.source)
      end

      render(conn, "index.json", message: message)
    else
      send_resp(conn, 406, "Nested values must be of the same type")
    end
  end

  def process_log(log_entry, %Source{} = source) do
    %{"message" => m, "metadata" => metadata, "timestamp" => ts, "level" => lv} = log_entry
    monotime = System.monotonic_time(:nanosecond)
    datetime = Timex.parse!(ts, "{ISO:Extended}")
    timestamp = Timex.to_unix(datetime) * 1_000_000
    unique_int = System.unique_integer([:monotonic])
    time_event = {timestamp, unique_int, monotime}

    metadata =
      metadata
      |> Injest.MetadataCleaner.deep_reject_nil_and_empty()
      |> Map.put("level", lv)

    send_with_data = &send_to_many_sources_by_rules(&1, time_event, m, metadata)

    if source.overflow_source && source_over_threshold?(source) do
      source_id = source.overflow_source |> String.to_atom()

      source_id
      |> Sources.Cache.get_by_id()
      |> send_with_data.()
    end

    send_with_data.(source)
  end

  defp send_to_many_sources_by_rules(%Source{} = source, time_event, log_entry, metadata) do
    rules = source.rules

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

    insert_log(source, time_event, log_entry, metadata)
  end

  defp insert_log(%Source{} = source, time_event, log_entry, metadata) do
    insert_and_broadcast(source, time_event, log_entry, metadata)
  end

  defp insert_and_broadcast(%Source{} = source, time_event, log_entry, metadata) do
    source_table_string = Atom.to_string(source.token)
    {timestamp, _unique_int, _monotime} = time_event

    payload = %{timestamp: timestamp, log_message: log_entry, metadata: metadata}

    Logs.insert_or_push(source.token, {time_event, payload})

    SourceBuffer.push(source_table_string, {time_event, payload})
    SourceCounter.incriment(source.token)
    SystemCounter.incriment(@system_counter)

    Logs.broadcast_log_count(source.token)
    Logs.broadcast_total_log_count()

    LogflareWeb.Endpoint.broadcast(
      "source:#{source.token}",
      "source:#{source.token}:new",
      payload
    )
  end

  defp source_over_threshold?(%Source{} = source) do
    current_rate = SourceData.get_rate(source.token)
    avg_rate = SourceData.get_avg_rate(source.token)

    avg_rate >= 1 and current_rate / 10 >= avg_rate
  end
end
