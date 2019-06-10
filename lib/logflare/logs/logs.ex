defmodule Logflare.Logs.Next do
  require Logger
  use Publicist
  alias Logflare.Validators.{DeepFieldTypes, BigQuerySchemaChange, BigQuerySchemaSpec}

  alias Logflare.{
    SystemCounter,
    Source,
    Sources
  }

  alias Logflare.Source.{BigQuery.Buffer, RecentLogsServer}
  alias Logflare.Logs.{Injest, RejectedEvents}
  alias Logflare.Sources.Counters
  alias Number.Delimit

  @system_counter :total_logs_logged

  @spec injest_logs(list(map), Source.t()) :: :ok | {:error, term}
  def injest_logs(batch, %Source{} = source) when is_list(batch) do
    batch
    |> Enum.map(&munge/1)
    |> Enum.map(&validate(&1, source))
    |> Enum.map(fn %{valid: valid} = log ->
      if valid, do: injest(log, source), else: RejectedEvents.injest(log)

      log
    end)
    |> Enum.reduce([], fn log, acc ->
      if log.valid do
        acc
      else
        [log.validation_error | acc]
      end
    end)
    |> case do
      [] -> :ok
      errors when is_list(errors) -> {:error, errors}
    end
  end

  defp munge(log_params) do
    metadata = log_params["metadata"]
    timestamp = log_params["timestamp"] || System.system_time(:microsecond)
    message = log_params["log_entry"] || log_params["message"]

    %{
      message: message,
      metadata: metadata,
      timestamp: timestamp
    }
    |> Injest.MetadataCleaner.deep_reject_nil_and_empty()
  end

  @spec validate(map(), Source.t()) :: map()
  def validate(log, source) when is_map(log) do
    [DeepFieldTypes, BigQuerySchemaSpec, BigQuerySchemaChange]
    |> Enum.reduce_while(true, fn validator, _acc ->
      case validator.validate(%{log: log, source: source}) do
        :ok ->
          {:cont, Map.put(log, :valid, true)}

        {:error, message} ->
          {:halt,
           log
           |> Map.put(:valid, false)
           |> Map.put(:validation_error, message)}
      end
    end)
  end

  defp injest(log_event, source) do
    injest_by_source_rules(log_event, source)
    injest_and_broadcast(log_event, source)
  end

  defp injest_by_source_rules(log_event, %Source{} = source) do
    for rule <- source.rules, Regex.match?(~r{#{rule.regex}}, "#{log_event.message}") do
      sink_source = Sources.Cache.get_by(token: rule.sink)

      if sink_source do
        injest_and_broadcast(sink_source, log_event)
      else
        Logger.error("Sink source for token UUID #{rule.sink} doesn't exist")
      end
    end
  end

  defp injest_and_broadcast(log_event, %Source{} = source) do
    %{
      message: message,
      metadata: metadata,
      timestamp: timestamp
    } = log_event

    source_table_string = Atom.to_string(source.token)

    payload =
      if metadata do
        %{timestamp: timestamp, log_message: message, metadata: metadata}
      else
        %{timestamp: timestamp, log_message: message}
      end

    time_event =
      timestamp
      |> build_time_event()

    time_log_event = {time_event, payload}

    if :ets.info(source.token) == :undefined do
      RecentLogsServer.push(source.token, time_log_event)
    else
      :ets.insert(source.token, time_log_event)
    end

    Buffer.push(source_table_string, time_log_event)
    Sources.Counters.incriment(source.token)
    SystemCounter.incriment(@system_counter)

    broadcast_log_count(source.token)
    broadcast_total_log_count()

    LogflareWeb.Endpoint.broadcast(
      "source:#{source.token}",
      "source:#{source.token}:new",
      payload
    )
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
