defmodule Logflare.Logs do
  use Publicist
  alias Logflare.Validator.{DeepFieldTypes, BigQuery}

  @dft_user_err "Metadata validation error: values with the same field path must have the same type."
  @bg_tm_user_err "Log entry metadata contains keys or values that are forbidden for the Google BigQuery table schema. Learn more at https://cloud.google.com/bigquery/docs/schemas"

  alias Logflare.{
    Table,
    TableCounter,
    SystemCounter,
    Sources,
    Source,
    SourceData,
    TableBuffer,
    Logs
  }

  alias Logflare.Logs.Injest

  alias Logflare.TableCounter
  alias Logflare.SystemCounter
  alias Number.Delimit

  @system_counter :total_logs_logged

  @spec insert_logs(list(map), Source.t()) :: :ok | {:error, term}
  def insert_logs(batch, %Source{} = source) when is_list(batch) do
    case validate_log_entries(batch) do
      :ok ->
        Enum.each(batch, &insert_log_to_source(&1, source))
        :ok

      {:invalid, reason} ->
        {:error, reason}
    end
  end

  @spec insert_or_push(atom(), {tuple(), map()}) :: true
  def insert_or_push(source_token, event) do
    if :ets.info(source_token) == :undefined do
      Table.push(source_token, event)
      true
    else
      :ets.insert(source_token, event)
    end
  end

  def broadcast_log_count(source_table) do
    {:ok, log_count} = TableCounter.get_total_inserts(source_table)
    source_table_string = Atom.to_string(source_table)

    payload = %{
      log_count: Delimit.number_to_delimited(log_count),
      source_token: source_table_string
    }

    LogflareWeb.Endpoint.broadcast(
      "dashboard:" <> source_table_string,
      "dashboard:#{source_table_string}:log_count",
      payload
    )
  end

  def broadcast_total_log_count() do
    {:ok, log_count} = SystemCounter.log_count(@system_counter)
    payload = %{total_logs_logged: Delimit.number_to_delimited(log_count)}

    LogflareWeb.Endpoint.broadcast("everyone", "everyone:update", payload)
  end

  @spec validate_log_entries(list(map)) :: :ok | {:invalid, term()}
  def validate_log_entries(batch) when is_list(batch) do
    Enum.reduce_while(
      batch,
      :ok,
      fn log_entry, _ ->
        case validate_log_entry(log_entry) do
          :ok -> {:cont, :ok}
          {:invalid, message} -> {:halt, {:error, message}}
        end
      end
    )
  end

  @spec validate_log_entry(map()) :: :ok | {:invalid, String.t()}
  def validate_log_entry(log_entry) when is_map(log_entry) do
    %{"metadata" => metadata} = log_entry

    with {:dft, true} <- {:dft, DeepFieldTypes.valid?(metadata)},
         {:bq_tm, true} <- {:bq_tm, BigQuery.TableMetadata.valid?(metadata)} do
      :ok
    else
      {:dft, false} -> {:invalid, @dft_user_err}
      {:bg_tm, false} -> {:invalid, @bg_tm_user_err}
    end
  end

  @spec build_time_event(String.t()| non_neg_integer) :: {non_neg_integer, integer, integer}
  defp build_time_event(timestamp) when is_integer(timestamp) do
    import System
    {timestamp, unique_integer([:monotonic]), monotonic_time(:nanosecond)}
  end

  defp build_time_event(iso_datetime) when is_binary(iso_datetime) do
    unix =
      iso_datetime
      |> Timex.parse!("{ISO:Extended}")
      |> Timex.to_unix()

    timestamp_mcs = unix * 1_000_000

    monotime = System.monotonic_time(:nanosecond)
    unique_int = System.unique_integer([:monotonic])
    {timestamp_mcs, unique_int, monotime}
  end

  defp insert_log_to_source(log_entry, %Source{} = source) do
    message = log_entry["log_entry"] || log_entry["message"]
    metadata = log_entry["metadata"] || %{}

    time_event =
      log_entry
      |> Map.get("timestamp", System.system_time(:microsecond))
      |> build_time_event()

    metadata =
      if lv = log_entry["level"] do
        Map.put(metadata, "level", lv)
      else
        metadata
      end
      |> Injest.MetadataCleaner.deep_reject_nil_and_empty()

    send_with_rules(source, time_event, message, metadata)
  end

  defp send_with_rules(%Source{} = source, time_event, log_message, metadata)
       when is_binary(log_message) do
    rules = source.rules

    Enum.each(
      rules,
      fn x ->
        if Regex.match?(~r{#{x.regex}}, "#{log_message}") do
          x.sink
          |> String.to_atom()
          |> insert_and_broadcast(time_event, log_message, metadata)
        end
      end
    )

    insert_and_broadcast(source, time_event, log_message, metadata)
  end

  defp insert_and_broadcast(%Source{} = source, time_event, log_message, metadata)
       when is_binary(log_message) do
    source_table_string = Atom.to_string(source.token)
    {timestamp, _unique_int, _monotime} = time_event

    payload = %{timestamp: timestamp, log_message: log_message, metadata: metadata}

    log_event = {time_event, payload}

    Logs.insert_or_push(source.token, log_event)

    TableBuffer.push(source_table_string, log_event)
    TableCounter.incriment(source.token)
    SystemCounter.incriment(@system_counter)

    broadcast_log_count(source.token)
    broadcast_total_log_count()

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
