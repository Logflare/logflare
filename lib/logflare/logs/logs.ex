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
    |> maybe_acc_errors()
    |> case do
      [] -> :ok
      errors when is_list(errors) -> {:error, errors}
    end
  end

  @spec ingest(Logflare.LogEvent.t()) :: Logflare.LogEvent.t() | {:error, term}
  def ingest(%LE{source: %Source{} = source} = le) do
    with {:ok, _} <- Supervisor.ensure_started(source.token),
         {:ok, _} <- BufferCounter.push(le),
         :ok <- RecentLogsServer.push(le),
         # tests fail when we match on these for some reason
         _ok <- Sources.Counters.increment(source.token),
         _ok <- SystemMetrics.AllLogsLogged.increment(:total_logs_logged) do
      le
    else
      {:error, _reason} = e ->
        e

      e ->
        {:error, e}
    end
  end

  @spec broadcast(Logflare.LogEvent.t()) :: Logflare.LogEvent.t()
  def broadcast(%LE{} = le) do
    if le.source.metrics.avg < 5 do
      Source.ChannelTopics.broadcast_new(le)
    end

    le
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

  @spec maybe_ingest_and_broadcast(Logflare.LogEvent.t()) :: Logflare.LogEvent.t()
  def maybe_ingest_and_broadcast(%LE{} = og_le) do
    with {:drop, false} <- {:drop, og_le.drop},
         {:valid, true} <- {:valid, og_le.valid},
         %LE{} = le <- LE.apply_custom_event_message(og_le),
         %LE{} = le <- ingest(le),
         %LE{} = le <- __MODULE__.broadcast(le),
         %LE{} = _le <- SourceRouting.route_to_sinks_and_ingest(og_le) do
      le
    else
      {:drop, true} ->
        og_le

      {:valid, false} ->
        tap(og_le, &RejectedLogEvents.ingest/1)

      {:error, :buffer_full} ->
        og_le
        |> Map.put(:valid, false)
        |> Map.put(:ingest_error, "buffer_full")

      e ->
        Logger.error("Unknown ingest error: " <> inspect(e))

        og_le
        |> Map.put(:valid, false)
        |> Map.put(:ingest_error, "unknown error")
    end
  end

  defp maybe_acc_errors(log_events) do
    Enum.reduce(log_events, [], fn le, acc ->
      cond do
        le.valid -> acc
        le.validation_error -> [Map.take(le, [:id, :validation_error]) | acc]
        le.ingest_error -> [Map.take(le, [:id, :ingest_error]) | acc]
      end
    end)
  end
end
