defmodule Logflare.Logs do
  @moduledoc false
  require Logger
  use Publicist

  alias Logflare.LogEvent, as: LE
  alias Logflare.Logs.{RejectedLogEvents}
  alias Logflare.{SystemMetrics, Source, Sources}
  alias Logflare.Source.{BigQuery.Buffer, RecentLogsServer}
  alias Logflare.Logs.SourceRouting
  alias Logflare.Logs.IngestTypecasting
  alias Logflare.Logs.IngestTransformers
  alias Logflare.Source.Supervisor

  @spec ingest_logs(list(map), Source.t()) :: :ok | {:error, term}
  def ingest_logs(log_params_batch, %Source{rules: rules} = source) when is_list(rules) do
    log_params_batch
    |> Enum.map(fn log ->
      log
      |> IngestTypecasting.maybe_apply_transform_directives()
      |> IngestTransformers.transform(:to_bigquery_column_spec)
      |> Map.put(:make_from, "ingest")
      |> LE.make(%{source: source})
      |> maybe_ingest_and_broadcast()
    end)
    |> Enum.reduce([], fn le, acc ->
      if le.valid do
        acc
      else
        [le.validation_error | acc]
      end
    end)
    |> case do
      [] -> :ok
      errors when is_list(errors) -> {:error, errors}
    end
  end

  def ingest(%LE{source: %Source{} = source} = le) do
    # indvididual source genservers
    Supervisor.ensure_started(source.token)
    RecentLogsServer.push(le)
    Buffer.push(le)

    # all sources genservers
    Sources.Counters.incriment(source.token)
    SystemMetrics.AllLogsLogged.incriment(:total_logs_logged)

    :ok
  end

  def broadcast(%LE{} = le) do
    if le.source.metrics.avg < 5 do
      Source.ChannelTopics.broadcast_new(le)
    end
  end

  defp maybe_ingest_and_broadcast(%LE{} = le) do
    if le.valid do
      le
      |> tap(fn x -> SourceRouting.route_to_sinks_and_ingest(x) end)
      |> LE.apply_custom_event_message()
      |> tap(fn x -> ingest(x) end)
      |> tap(fn x -> broadcast(le) end)
    else
      le
      |> tap(fn x -> RejectedLogEvents.ingest(x) end)
    end
  end
end
