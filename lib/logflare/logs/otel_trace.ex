defmodule Logflare.Logs.OtelTrace do
  @moduledoc """
  Converts a list of Otel ResourceSpans to a list of Logflare events

  * One ResourceSpans can contain multiple ScopeSpans
  * One ScopeSpans can contain multiple Spans
  * One Span can contain multiple Events
  * A Log Event is created for each Span and each Event
  """

  alias Logflare.Logs.Otel

  alias Opentelemetry.Proto.Trace.V1.ResourceSpans
  @behaviour Logflare.Logs.Processor

  def handle_batch(resource_spans, _source) when is_list(resource_spans) do
    resource_spans
    |> Enum.map(&handle_resource_span/1)
    |> List.flatten()
  end

  defp handle_resource_span(%ResourceSpans{resource: resource, scope_spans: scope_spans}) do
    resource = Otel.handle_resource(resource)
    Enum.map(scope_spans, &handle_scope_span(&1, resource))
  end

  defp handle_scope_span(%{scope: scope, spans: spans}, resource) do
    scope = Otel.handle_scope(scope)
    Enum.map(spans, &handle_span(&1, resource, scope))
  end

  defp handle_span(span, resource, scope) do
    metadata = %{"type" => "span"}
    events = Enum.map(span.events, &handle_event(&1, span, resource, scope))

    [
      %{
        "event_message" => span.name,
        "metadata" => metadata,
        "resource" => resource,
        "scope" => scope,
        "span_id" => Base.encode16(span.span_id, case: :lower),
        "parent_span_id" => Base.encode16(span.parent_span_id, case: :lower),
        "trace_id" => Base.encode16(span.trace_id, case: :lower),
        "start_time" => span.start_time_unix_nano,
        "end_time" => span.end_time_unix_nano,
        "attributes" => Otel.handle_attributes(span.attributes),
        "timestamp" => span.start_time_unix_nano,
        "project" => Otel.resource_project(resource)
      }
    ] ++ events
  end

  defp handle_event(event, %{span_id: span_id, trace_id: trace_id}, resource, scope) do
    metadata = %{"type" => "event"}

    %{
      "event_message" => event.name,
      "metadata" => metadata,
      "resource" => resource,
      "scope" => scope,
      "parent_span_id" => Base.encode16(span_id),
      "trace_id" => Base.encode16(trace_id, case: :lower),
      "attributes" => Otel.handle_attributes(event.attributes),
      "timestamp" => event.time_unix_nano,
      "project" => Otel.resource_project(resource)
    }
  end
end
