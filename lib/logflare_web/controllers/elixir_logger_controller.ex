defmodule LogflareWeb.ElixirLoggerController do
  use LogflareWeb, :controller

  alias Logflare.Sources
  alias Logflare.TableCounter
  alias Logflare.SystemCounter
  alias Logflare.TableManager
  alias Logflare.SourceData
  alias Logflare.TableBuffer
  alias Logflare.Logs

  @system_counter :total_logs_logged

  def create(conn, %{"batch" => batch, "source" => source_id}) do
    message = "Logged!"

    for log_entry <- batch do
      process_log(log_entry, %{source_id: String.to_atom(source_id)})
    end

    render(conn, "index.json", message: message)
  end

  def process_log(log_entry, %{source_id: source_id}) when is_atom(source_id) do
    %{"message" => m, "metadata" => metadata, "timestamp" => ts, "level" => lv} = log_entry
    monotime = System.monotonic_time(:nanosecond)
    datetime = Timex.parse!(ts, "{ISO:Extended}")
    timestamp = Timex.to_unix(datetime) * 1_000_000
    unique_int = System.unique_integer([:monotonic])
    time_event = {timestamp, unique_int, monotime}

    metadata = metadata |> Map.put("level", lv)

    source = Sources.Cache.get_by_id(source_id)

    %{overflow_source: overflow_source} = source

    send_with_data = &send_to_many_sources_by_rules(&1, time_event, m, metadata)

    if overflow_source && source_over_threshold?(source.token) do
      overflow_source
      |> String.to_atom()
      |> send_with_data.()
    end

    send_with_data.(source_id)
  end

  defp send_to_many_sources_by_rules(source_id, time_event, log_entry, metadata)
       when is_atom(source_id) do
    rules = Sources.Cache.get_by_id(source_id).rules

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

    insert_log(source_id, time_event, log_entry, metadata)
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

    payload = %{timestamp: timestamp, log_message: log_entry, metadata: metadata}

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

  defp source_over_threshold?(source_id) when is_atom(source_id) do
    current_rate = SourceData.get_rate(source_id)
    avg_rate = SourceData.get_avg_rate(source_id)

    avg_rate >= 1 and current_rate / 10 >= avg_rate
  end
end
