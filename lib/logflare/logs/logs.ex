defmodule Logflare.Logs do
  use Publicist
  alias Logflare.Validator.{DeepFieldTypes, BigQuery}

  alias Logflare.{
    SystemCounter,
    Source
  }

  alias Logflare.Source.{BigQuery.Buffer, RecentLogsServer}
  alias Logflare.Logs.Injest
  alias Logflare.Sources.Counters
  alias Number.Delimit

  @system_counter :total_logs_logged

  @spec insert_logs(list(map), Source.t()) :: :ok | {:error, term}
  def insert_logs(batch, %Source{} = source) when is_list(batch) do
    case validate_batch_params(batch) do
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
      RecentLogsServer.push(source_token, event)
      true
    else
      :ets.insert(source_token, event)
    end
  end

  @spec validate_batch_params(list(map)) :: :ok | {:invalid, term()}
  def validate_batch_params(batch) when is_list(batch) do
    reducer = fn log_entry, _ ->
      case validate_params(log_entry) do
        :ok -> {:cont, :ok}
        invalid_tup -> {:halt, invalid_tup}
      end
    end

    Enum.reduce_while(
      batch,
      :ok,
      reducer
    )
  end

  @spec validate_params(map()) :: :ok | {:invalid, atom}
  def validate_params(log_entry) when is_map(log_entry) do
    metadata = log_entry["metadata"] || %{}

    validators = [DeepFieldTypes, BigQuery.TableMetadata]

    Enum.reduce_while(validators, true, fn validator, _ ->
      if validator.valid?(metadata) do
        {:cont, :ok}
      else
        {:halt, {:invalid, validator}}
      end
    end)
  end

  @spec build_time_event(String.t() | non_neg_integer) :: {non_neg_integer, integer, integer}
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
    for rule <- source.rules do
      if Regex.match?(~r{#{rule.regex}}, "#{log_message}") do
        rule.sink
        |> insert_and_broadcast(time_event, log_message, metadata)
      end
    end

    insert_and_broadcast(source, time_event, log_message, metadata)
  end

  defp insert_and_broadcast(%Source{} = source, time_event, log_message, metadata)
       when is_binary(log_message) do
    source_table_string = Atom.to_string(source.token)

    {timestamp, _unique_int, _monotime} = time_event
    payload = %{timestamp: timestamp, log_message: log_message, metadata: metadata}
    log_event = {time_event, payload}

    insert_or_push(source.token, log_event)

    Buffer.push(source_table_string, log_event)
    Counters.incriment(source.token)
    SystemCounter.incriment(@system_counter)

    broadcast_log_count(source.token)
    broadcast_total_log_count()

    LogflareWeb.Endpoint.broadcast(
      "source:#{source.token}",
      "source:#{source.token}:new",
      payload
    )
  end

  def broadcast_log_count(source_table) do
    {:ok, log_count} = Counters.get_total_inserts(source_table)
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

  @spec broadcast_total_log_count() :: :ok | {:error, any()}
  def broadcast_total_log_count() do
    {:ok, log_count} = SystemCounter.log_count(@system_counter)
    payload = %{total_logs_logged: Delimit.number_to_delimited(log_count)}

    LogflareWeb.Endpoint.broadcast("everyone", "everyone:update", payload)
  end
end
