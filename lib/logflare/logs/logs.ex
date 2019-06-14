defmodule Logflare.Logs do
  require Logger
  use Publicist

  alias Logflare.LogEvent
  alias Logflare.LogEvent, as: LE

  alias Logflare.{SystemCounter, Source, Sources}
  alias Logflare.Source.{BigQuery.Buffer, RecentLogsServer}
  alias Logflare.Logs.{RejectedLogEvents}
  alias Logflare.Sources.Counters
  alias Number.Delimit

  @system_counter :total_logs_logged

  @spec injest_logs(list(map), Source.t()) :: :ok | {:error, term}
  def injest_logs(log_params_batch, %Source{} = source) do
    log_params_batch
    |> Enum.map(&LogEvent.make(&1, %{source: source}))
    |> Enum.map(fn %LE{} = log_event ->
      if log_event.valid? do
        injest(log_event)
      else
        RejectedLogEvents.injest(log_event)
      end

      log_event
    end)
    |> Enum.reduce([], fn log, acc ->
      if log.valid? do
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

  defp injest(%LE{} = log_event) do
    injest_by_source_rules(log_event, log_event.source)
    injest_and_broadcast(log_event, log_event.source)
  end

  defp injest_by_source_rules(log_event, %Source{} = source) do
    for rule <- source.rules, Regex.match?(~r{#{rule.regex}}, "#{log_event.message}") do
      sink_source = Sources.Cache.get_by(token: rule.sink)

      if sink_source do
        # FIXME double counting log event counts here
        injest_and_broadcast(sink_source, log_event)
      else
        Logger.error("Sink source for token UUID #{rule.sink} doesn't exist")
      end
    end
  end

  defp injest_and_broadcast(log_event, %Source{} = source) do
    source_table_string = Atom.to_string(source.token)

    {message, body} = Map.pop(log_event.body, :message)

    payload =
      body
      |> Map.put(:log_message, message)
      |> Map.from_struct()

    time_event = build_time_event(body.timestamp)
    time_log_event = {time_event, payload}

    RecentLogsServer.push(source.token, time_log_event)
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
