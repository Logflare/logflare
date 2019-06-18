defmodule Logflare.Logs do
  require Logger
  use Publicist

  alias Logflare.LogEvent
  alias Logflare.LogEvent, as: LE

  alias Logflare.{SystemMetrics, Source, Sources}

  alias Logflare.Source.{BigQuery.Buffer, RecentLogsServer}
  alias Logflare.Logs.{RejectedLogEvents}
  alias Logflare.Sources.Counters
  alias Number.Delimit

  @spec injest_logs(list(map), Source.t()) :: :ok | {:error, term}
  def injest_logs(log_params_batch, %Source{} = source) do
    log_params_batch
    |> Enum.map(&LogEvent.make(&1, %{source: source}))
    |> Enum.map(fn %LE{} = log_event ->
      if log_event.valid? do
        injest_by_source_rules(log_event)
        injest_and_broadcast(log_event)
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

  @spec injest_by_source_rules(LE.t()) :: term
  defp injest_by_source_rules(%LE{source: %Source{} = source} = log_event) do
    for rule <- source.rules, Regex.match?(~r{#{rule.regex}}, log_event.body.message) do
      sink_source = Sources.Cache.get_by(token: rule.sink)

      if sink_source do
        injest_and_broadcast(%{log_event | source: sink_source})
      else
        Logger.error("Sink source for token UUID #{rule.sink} doesn't exist")
      end
    end
  end

  defp injest_and_broadcast(%LE{source: %Source{} = source} = log_event) do
    source_table_string = Atom.to_string(source.token)

    RecentLogsServer.push(source.token, log_event)
    Buffer.push(source_table_string, log_event)
    Sources.Counters.incriment(source.token)
    SystemMetrics.AllLogsLogged.incriment(:total_logs_logged)

    broadcast_log_count(source)

    {message, body} = Map.pop(log_event.body, :message)

    payload =
      body
      |> Map.put(:log_message, message)
      |> Map.from_struct()

    LogflareWeb.Endpoint.broadcast(
      "source:#{source.token}",
      "source:#{source.token}:new",
      payload
    )
  end

  def broadcast_log_count(%Source{token: source_id} = source) do
    {:ok, log_count} = Counters.get_total_inserts(source_id)
    source_table_string = Atom.to_string(source_id)

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
end
