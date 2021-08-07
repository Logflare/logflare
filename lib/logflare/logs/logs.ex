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
    # This should get refreshed in the LogflareWeb.Plugs.SetVerifySource
    source = Sources.refresh_source_metrics(source)

    log_params_batch
    |> Enum.map(&IngestTypecasting.maybe_apply_transform_directives/1)
    |> Enum.map(&IngestTransformers.transform(&1, :to_bigquery_column_spec))
    |> Enum.map(&Map.put(&1, :make_from, "ingest"))
    |> Enum.map(&LE.make(&1, %{source: source}))
    |> Enum.map(fn %LE{} = le ->
      if le.valid do
        SourceRouting.route_to_sinks_and_ingest(le)
        LE.apply_custom_event_message(le)
        ingest(le)
        broadcast(le)
      else
        RejectedLogEvents.ingest(le)
      end

      le
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
end
