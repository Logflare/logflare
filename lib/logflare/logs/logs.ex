defmodule Logflare.Logs do
  @moduledoc false
  require Logger

  alias Logflare.LogEvent
  alias Logflare.Logs.IngestTransformers
  alias Logflare.Logs.IngestTypecasting
  alias Logflare.Logs.RejectedLogEvents
  alias Logflare.Logs.SourceRouting
  alias Logflare.Rule
  alias Logflare.Source
  alias Logflare.Source.BigQuery.BufferCounter
  alias Logflare.Source.RecentLogsServer
  alias Logflare.Source.Supervisor
  alias Logflare.Sources
  alias Logflare.SystemMetrics

  @spec ingest_logs(list(map), Source.t()) :: :ok | {:error, term}
  def ingest_logs(log_params_batch, %Source{rules: rules} = source)
      when is_list(rules) do
    log_params_batch
    |> Enum.map(fn log ->
      log
      |> IngestTypecasting.maybe_apply_transform_directives()
      |> IngestTransformers.transform(:to_bigquery_column_spec)
      |> LogEvent.make(%{source: source})
      |> maybe_mark_le_dropped_by_lql()
      |> maybe_ingest_and_broadcast()
    end)
    |> Enum.reduce([], fn
      %{valid: true}, acc -> acc
      le, acc -> [le.validation_error | acc]
    end)
    |> then(fn
      [] -> :ok
      errors when is_list(errors) -> {:error, errors}
    end)
  end

  def ingest(%LogEvent{source: %Source{} = source} = le) do
    case Logflare.SingleTenant.supabase_mode?() do
      true -> ingest(:backends, source, le)
      false -> ingest(:bigquery, source, le)
    end
  end

  defp ingest(:bigquery, source, le) do
    # indvididual source genservers
    Supervisor.ensure_started(source.token)

    # error here if this doesn't match
    {:ok, _} = BufferCounter.push(le)

    RecentLogsServer.push(le)

    # all sources genservers

    Sources.Counters.increment(source.token)
    SystemMetrics.AllLogsLogged.increment(:total_logs_logged)
    :ok
  end

  defp ingest(:backends, source, le) do
    Logflare.Backends.ingest_logs([le], source)
  end

  def broadcast(%LogEvent{} = le) do
    if le.source.metrics.avg < 5 do
      Source.ChannelTopics.broadcast_new(le)
    end
  end

  def maybe_mark_le_dropped_by_lql(%LogEvent{source: %{drop_lql_string: drop_lql_string}} = le)
      when is_nil(drop_lql_string) do
    le
  end

  def maybe_mark_le_dropped_by_lql(
        %LogEvent{
          body: _body,
          source: %{drop_lql_string: drop_lql_string, drop_lql_filters: filters}
        } = le
      )
      when is_binary(drop_lql_string) do
    cond do
      length(filters) >= 1 &&
          SourceRouting.route_with_lql_rules?(le, %Rule{lql_filters: filters}) ->
        Map.put(le, :drop, true)

      true ->
        le
    end
  end

  defp maybe_ingest_and_broadcast(%LogEvent{} = le) do
    cond do
      le.drop ->
        le

      le.valid ->
        le
        |> tap(&SourceRouting.route_to_sinks_and_ingest/1)
        |> LogEvent.apply_custom_event_message()
        |> tap(&ingest/1)
        # use module reference namespace for Mimic mocking
        |> tap(&__MODULE__.broadcast/1)

      true ->
        le
        |> tap(&RejectedLogEvents.ingest/1)
    end
  end
end
