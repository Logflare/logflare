defmodule Logflare.Logs.OtelTrace do
  @moduledoc """
  Converts a GRPC Trace to event parameters

  Converts a list of ResourceSpans to a list of Logflare Events
  Important details about the conversation:
  * One ResourceSpans can contain multiple ScopeSpans
  * One ScopeSpans can contain multiple Spans
  * One Span can contain multiple Events
  * A Log Event is created for each Span and each Event

  """
  require Logger

  alias Logflare.Logs.Otel

  alias Opentelemetry.Proto.Trace.V1.ResourceSpans
  @behaviour Logflare.Logs.Processor

  def handle_batch(resource_spans, _source) when is_list(resource_spans) do
    Enum.map(resource_spans, &handle_event/1)
    |> List.flatten()
  end

  defp handle_event(%ResourceSpans{resource: resource, scope_spans: scope_spans}) do
    resource = Otel.handle_resource(resource)

    scope_spans
    |> Enum.map(&handle_scope_span(&1, resource))
    |> List.flatten()
  end

  defp handle_scope_span(%{scope: scope, spans: spans}, resource) do
    resource = Otel.merge_scope_attributes(resource, scope)
    Enum.map(spans, &handle_span(&1, resource))
  end

  defp handle_span(span, resource) do
    start_time = Otel.nano_to_iso8601(span.start_time_unix_nano)
    metadata = %{"type" => "span"}
    metadata = Map.merge(metadata, resource)
    events = Enum.map(span.events, &handle_event(&1, span, resource))

    [
      %{
        "event_message" => span.name,
        "metadata" => metadata,
        "span_id" => Base.encode16(span.span_id),
        "parent_span_id" => Base.encode16(span.parent_span_id),
        "trace_id" => Ecto.UUID.cast!(span.trace_id),
        "start_time" => start_time,
        "end_time" => Otel.nano_to_iso8601(span.end_time_unix_nano),
        "attributes" => Otel.handle_attributes(span.attributes),
        "timestamp" => start_time,
        "project" => resource["name"]
      }
    ] ++ events
  end

  defp handle_event(event, %{span_id: span_id, trace_id: trace_id}, resource) do
    metadata = %{"type" => "event"}
    metadata = Map.merge(metadata, resource)

    %{
      "event_message" => event.name,
      "metadata" => metadata,
      "parent_span_id" => Base.encode16(span_id),
      "trace_id" => Ecto.UUID.cast!(trace_id),
      "attributes" => Otel.handle_attributes(event.attributes),
      "timestamp" => Otel.nano_to_iso8601(event.time_unix_nano)
    }
  end
end
