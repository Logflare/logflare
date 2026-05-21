defmodule Logflare.Backends.UserMonitoring.SpanProcessor do
  @moduledoc """
  OTel span processor that routes completed user-related spans to `system.traces` sources.

  Excluded from routing:
  - Spans from ingestion API paths (high-volume, not useful for system monitoring)
  - Spans from system sources (prevents feedback loops)
  - Spans with no resolvable user context
  """

  @behaviour :otel_span_processor

  require Record

  alias Logflare.Logs
  alias Logflare.Logs.Processor
  alias Logflare.Sources
  alias Logflare.Users

  @span_fields Record.extract(:span, from: "deps/opentelemetry/include/otel_span.hrl")
  Record.defrecordp(:span, @span_fields)

  # URL path prefixes that identify ingestion routes — excluded from system.traces
  @ingestion_path_prefixes ["/logs", "/api/logs", "/api/events", "/open-telemetry"]

  # Span name prefixes that identify ingestion processing — excluded from system.traces
  @ingestion_name_prefixes ["ingest."]

  @impl :otel_span_processor
  def on_start(_ctx, span, _config), do: span

  @impl :otel_span_processor
  def on_end(span_record, _config) do
    attrs = span_attrs(span_record)

    unless ingestion_span?(span_record, attrs) or system_source_span?(attrs) do
      case Users.get_related_user_id(attrs) do
        user_id when is_integer(user_id) ->
          route_span(span_record, attrs, user_id)

        _ ->
          :ok
      end
    end

    true
  end

  @impl :otel_span_processor
  def force_flush(_config), do: :ok

  @impl :otel_span_processor
  def shutdown(_timeout, _config), do: :ok

  defp ingestion_span?(span_record, attrs) do
    name = span_record |> span(:name) |> to_string()
    url_path = Map.get(attrs, "url.path", "")

    Enum.any?(@ingestion_name_prefixes, &String.starts_with?(name, &1)) or
      Enum.any?(@ingestion_path_prefixes, &String.starts_with?(url_path, &1))
  end

  defp system_source_span?(attrs), do: Map.get(attrs, "system_source") == true

  defp route_span(span_record, attrs, user_id) do
    with %{system_monitoring: true} <- Users.Cache.get(user_id),
         %Sources.Source{} = source <- get_traces_source(user_id) do
      span_record
      |> span_to_event(attrs)
      |> List.wrap()
      |> Processor.ingest(Logs.Raw, source)
    end
  end

  defp get_traces_source(user_id) do
    Sources.Cache.get_by_and_preload_rules(user_id: user_id, system_source_type: :traces)
    |> Sources.refresh_source_metrics_for_ingest()
  end

  defp span_to_event(span_record, attrs) do
    %{
      "event_message" => span_record |> span(:name) |> to_string(),
      "metadata" => %{"type" => "span"},
      "trace_id" => encode_id(span(span_record, :trace_id)),
      "span_id" => encode_id(span(span_record, :span_id)),
      "parent_span_id" => encode_id(span(span_record, :parent_span_id)),
      "start_time" => to_ns(span(span_record, :start_time)),
      "end_time" => to_ns(span(span_record, :end_time)),
      "attributes" => attrs,
      "timestamp" => to_ns(span(span_record, :start_time))
    }
  end

  defp span_attrs(span_record) do
    span_record
    |> span(:attributes)
    |> :otel_attributes.map()
    |> Map.new(fn {k, v} -> {to_string(k), v} end)
  rescue
    _ -> %{}
  end

  defp encode_id(0), do: ""

  defp encode_id(id) when is_integer(id) do
    id |> :binary.encode_unsigned() |> Base.encode16(case: :lower)
  end

  defp encode_id(_), do: ""

  defp to_ns({_, ns}), do: ns
  defp to_ns(ns) when is_integer(ns), do: ns
  defp to_ns(_), do: 0
end
