defmodule Logflare.Logs do
  @moduledoc false
  require Logger

  alias Logflare.LogEvent, as: LE
  alias Logflare.Logs.{RejectedLogEvents}
  alias Logflare.{SystemMetrics, Source, Sources}
  alias Logflare.Source.{BigQuery.BufferCounter, RecentLogsServer}
  alias Logflare.Logs.SourceRouting
  alias Logflare.Logs.IngestTypecasting
  alias Logflare.Logs.IngestTransformers
  alias Logflare.Source.Supervisor
  alias Logflare.Rule

  @spec ingest_logs(list(map), Source.t()) :: :ok | {:error, term}
  def ingest_logs(log_params_batch, %Source{rules: rules} = source) when is_list(rules) do
    log_params_batch
    |> Enum.map(fn log ->
      log
      |> IngestTypecasting.maybe_apply_transform_directives()
      |> IngestTransformers.transform(:to_bigquery_column_spec)
      |> LE.make(%{source: source})
      |> maybe_mark_le_dropped_by_lql()
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
    BufferCounter.push(le)

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

  def maybe_mark_le_dropped_by_lql(%LE{source: %{drop_lql_string: drop_lql_string}} = le)
      when is_nil(drop_lql_string) do
    le
  end

  def maybe_mark_le_dropped_by_lql(
        %LE{body: _body, source: %{drop_lql_string: drop_lql_string, drop_lql_filters: filters}} =
          le
      )
      when is_binary(drop_lql_string) do
    cond do
      length(filters) >= 1 && SourceRouting.route_with_lql_rules?(le, %Rule{lql_filters: filters}) ->
        Map.put(le, :drop, true)

      true ->
        le
    end
  end

  defp maybe_ingest_and_broadcast(%LE{} = le) do
    cond do
      le.drop ->
        le

      le.valid ->
        le
        |> tap(&SourceRouting.route_to_sinks_and_ingest/1)
        |> LE.apply_custom_event_message()
        |> tap(&ingest/1)
        # use module reference namespace for Mimic mocking
        |> tap(&__MODULE__.broadcast/1)

      true ->
        le
        |> tap(&RejectedLogEvents.ingest/1)
    end
  end
end
