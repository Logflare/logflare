defmodule Logflare.LogEvent.TypeDetection do
  @moduledoc """
  Determine whether raw ingestion params represent a log, metric, or trace."
  """

  import Logflare.Utils.Guards, only: [is_non_empty_binary: 1]

  @type event_type :: :log | :metric | :trace

  @doc """
  Determines the log type from raw ingestion params.

  Checks `metadata.type` first (set by OTEL processors), then falls back
  to heuristic key detection for raw JSON payloads. Defaults to `:log`.
  """
  @spec detect(map()) :: event_type()
  def detect(%{"metadata" => %{"type" => "span"}}), do: :trace
  def detect(%{"metadata" => %{"type" => "metric"}}), do: :metric
  def detect(%{"metadata" => %{"type" => "event"}}), do: :log
  def detect(%{"metadata" => %{"type" => "otel_log"}}), do: :log

  def detect(params) when is_map(params) do
    cond do
      trace?(params) -> :trace
      metric?(params) -> :metric
      true -> :log
    end
  end

  @doc """
  Whether `start_time`/`end_time` fields in the params hold OpenTelemetry
  unix-nanosecond timestamps.

  Single definition shared by BigQuery schema typing
  (`Logflare.Sources.Source.BigQuery.SchemaBuilder`) and value conversion
  (`Logflare.Google.BigQuery.EventUtils.convert_to_seconds/1`) so the column
  type and the ingested value always agree.
  """
  @spec otel_timestamps?(map()) :: boolean()
  def otel_timestamps?(params) when is_map(params) do
    detect(params) in [:metric, :trace] or otel_shaped?(params)
  end

  defp otel_shaped?(params) do
    Map.has_key?(params, "resource") and Map.has_key?(params, "scope")
  end

  defp trace?(params) do
    has_trace_id?(params) and has_span_id?(params) and has_span_field?(params)
  end

  defp has_trace_id?(params) do
    is_non_empty_binary(params["trace_id"]) or
      is_non_empty_binary(params["traceId"]) or
      is_non_empty_binary(params["otel_trace_id"])
  end

  defp has_span_id?(params) do
    is_non_empty_binary(params["span_id"]) or
      is_non_empty_binary(params["spanId"]) or
      is_non_empty_binary(params["otel_span_id"])
  end

  defp has_span_field?(params) do
    is_non_empty_binary(params["parent_span_id"]) or
      is_non_empty_binary(params["parentSpanId"]) or
      is_non_empty_binary(params["start_time"]) or
      is_non_empty_binary(params["end_time"]) or
      is_non_empty_binary(params["duration"]) or
      is_non_empty_binary(params["duration_ms"]) or
      is_non_empty_binary(params["duration_ns"])
  end

  defp metric?(params) do
    has_metric_name?(params) and has_metric_value?(params)
  end

  defp has_metric_name?(params) do
    is_non_empty_binary(params["metric_type"]) or
      is_non_empty_binary(params["metric"])
  end

  defp has_metric_value?(params) do
    value = params["value"] || params["gauge"] || params["count"] || params["sum"]
    value != nil
  end
end
