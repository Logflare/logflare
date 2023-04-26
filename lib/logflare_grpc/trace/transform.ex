defmodule LogflareGrpc.Trace.Transform do
  @moduledoc """
  Converts a GRPC Trace to a Logflare Event
  """

  alias Opentelemetry.Proto.Trace.V1.ResourceSpans
  alias Opentelemetry.Proto.Common.V1.KeyValue
  alias Logflare.LogEvent
  alias Logflare.Source

  @doc """
  Converts a list of ResourceSpans to a list of Logflare Events
  Important details about the conversation:
  * One ResourceSpans can contain multiple ScopeSpans
  * One ScopeSpans can contain multiple Spans
  * One Span can contain multiple Events
  * A Log Event is created for each Span and each Event
  """
  @spec to_log_events(list(ResourceSpans.t()), Source.t()) :: list(LogEvent.t())
  def to_log_events(resource_spans, source) do
    resource_spans
    |> Enum.map(&to_log_event(&1, source))
    |> List.flatten()
  end

  defp to_log_event(%ResourceSpans{} = span, source) do
    span
    |> handle_resource_span()
    |> Enum.map(&LogEvent.make(&1, %{source: source}))
  end

  defp handle_resource_span(%{resource: resource, scope_spans: scope_spans}) do
    resource = handle_resource(resource)

    scope_spans
    |> Enum.map(&handle_scope_span(&1, resource))
    |> List.flatten()
  end

  defp handle_resource(%{attributes: attributes}) do
    attributes
    |> Enum.map(&extract_key_value/1)
    |> Enum.map(fn {k, v} -> {k |> String.split(".") |> Enum.reverse(), v} end)
    |> Enum.map(fn {k, v} -> Enum.reduce(k, v, fn key, acc -> %{key => acc} end) end)
    |> Enum.reduce(%{}, fn map, acc -> DeepMerge.deep_merge(map, acc) end)
  end

  defp handle_scope_span(%{scope: scope, spans: spans}, resource) do
    %{name: name, version: version, attributes: attributes} = scope
    resource = Map.merge(resource, %{"name" => name, "version" => version})
    resource = Map.merge(resource, handle_attributes(attributes))

    Enum.map(spans, &handle_span(&1, resource))
  end

  defp handle_span(span, resource) do
    start_time =
      DateTime.from_unix!(span.start_time_unix_nano, :nanosecond) |> DateTime.to_iso8601()

    metadata = %{"type" => "span"}
    metadata = Map.merge(metadata, resource)

    events = Enum.map(span.events, &handle_event(&1, span, resource))

    [
      %{
        "message" => span.name,
        "metadata" => metadata,
        "span_id" => Base.encode16(span.span_id),
        "parent_span_id" => Base.encode16(span.parent_span_id),
        "trace_id" => Ecto.UUID.cast!(span.trace_id),
        "start_time" => start_time,
        "end_time" =>
          DateTime.from_unix!(span.end_time_unix_nano, :nanosecond) |> DateTime.to_iso8601(),
        "attributes" => handle_attributes(span.attributes),
        "timestamp" => start_time,
        "project" => resource["name"]
      }
    ] ++ events
  end

  defp handle_event(event, %{span_id: span_id, trace_id: trace_id}, resource) do
    metadata = %{"type" => "event"}
    metadata = Map.merge(metadata, resource)

    %{
      "message" => event.name,
      "metadata" => metadata,
      "parent_span_id" => Base.encode16(span_id),
      "trace_id" => Ecto.UUID.cast!(trace_id),
      "attributes" => handle_attributes(event.attributes),
      "timestamp" =>
        DateTime.from_unix!(event.time_unix_nano, :nanosecond) |> DateTime.to_iso8601()
    }
  end

  defp handle_attributes(attributes) do
    attributes
    |> Enum.map(&extract_key_value/1)
    |> Map.new()
  end

  defp extract_key_value(%KeyValue{key: key, value: nil}), do: {key, nil}

  defp extract_key_value(%KeyValue{key: key, value: %{value: {_, value}}}) do
    {key, value}
  end
end
