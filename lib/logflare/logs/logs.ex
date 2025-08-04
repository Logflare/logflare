defmodule Logflare.Logs do
  @moduledoc false
  require Logger

  alias Logflare.Backends.IngestEventQueue
  alias Logflare.LogEvent, as: LE
  alias Logflare.Logs.IngestTypecasting
  alias Logflare.Logs.RejectedLogEvents
  alias Logflare.Logs.SourceRouting
  alias Logflare.Rules.Rule
  alias Logflare.Source
  alias Logflare.Sources
  alias Logflare.SystemMetrics

  @spec ingest_logs(list(map), Source.t()) :: :ok | {:error, term}
  def ingest_logs(log_params_batch, %Source{rules: rules} = source) when is_list(rules) do
    Logger.metadata(source_id: source.token, source_token: source.token)

    log_params_batch
    |> Enum.map(fn log ->
      log
      |> IngestTypecasting.maybe_apply_transform_directives()
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
    with :ok <- Source.Supervisor.ensure_started(source),
         # tests fail when we match on these for some reason
         _ok <- Sources.Counters.increment(source.token),
         _ok <- SystemMetrics.AllLogsLogged.increment(:total_logs_logged) do
      # v1 only
      IngestEventQueue.add_to_table({source.id, nil}, [le])

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

  def maybe_mark_le_dropped_by_lql(%LE{source: %{drop_lql_string: nil}} = le), do: le
  def maybe_mark_le_dropped_by_lql(%LE{source: %{drop_lql_string: ""}} = le), do: le
  def maybe_mark_le_dropped_by_lql(%LE{source: %{drop_lql_filters: []}} = le), do: le

  def maybe_mark_le_dropped_by_lql(%LE{source: %{drop_lql_filters: filters}} = le) do
    if SourceRouting.route_with_lql_rules?(le, %Rule{lql_filters: filters}) do
      %{le | drop: true}
    else
      le
    end
  end

  @spec maybe_ingest_and_broadcast(Logflare.LogEvent.t()) :: Logflare.LogEvent.t()
  def maybe_ingest_and_broadcast(%LE{} = le) do
    with {:drop, false} <- {:drop, le.drop},
         {:valid, true} <- {:valid, le.valid},
         %LE{} = le <- LE.apply_custom_event_message(le),
         %LE{} = le <- ingest(le),
         %LE{} = le <- __MODULE__.broadcast(le),
         %LE{} = le <- SourceRouting.route_to_sinks_and_ingest(le) do
      le
    else
      {:drop, true} ->
        :telemetry.execute(
          [:logflare, :logs, :ingest_logs],
          %{drop: true},
          %{source_id: le.source.id, source_token: le.source.token}
        )

        le

      {:valid, false} ->
        :telemetry.execute(
          [:logflare, :logs, :ingest_logs],
          %{rejected: true},
          %{source_id: le.source.id, source_token: le.source.token}
        )

        RejectedLogEvents.ingest(le)

        le

      {:error, :buffer_full} ->
        :telemetry.execute(
          [:logflare, :logs, :ingest_logs],
          %{buffer_full: true},
          %{source_id: le.source.id, source_token: le.source.token}
        )

        le
        |> Map.put(:valid, false)
        |> Map.put(:pipeline_error, %LE.PipelineError{
          stage: "ingest",
          type: "buffer_full",
          message: "Source buffer full, please try again in a minute."
        })

      e ->
        Logger.error("Unknown ingest error: " <> inspect(e))

        le
        |> Map.put(:valid, false)
        |> Map.put(:pipeline_error, %LE.PipelineError{
          stage: "ingest",
          type: "unknown_error",
          message: "An unknown error has occured, please contact support if this continues."
        })
    end
  end

  defp maybe_acc_errors(log_events) do
    Enum.reduce(log_events, [], fn le, acc ->
      cond do
        le.valid -> acc
        le.pipeline_error -> [le.pipeline_error.message | acc]
      end
    end)
  end
end
