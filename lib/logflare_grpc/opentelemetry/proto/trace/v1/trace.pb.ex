defmodule Opentelemetry.Proto.Trace.V1.SpanFlags do
  @moduledoc false

  use Protobuf, enum: true, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :SPAN_FLAGS_DO_NOT_USE, 0
  field :SPAN_FLAGS_TRACE_FLAGS_MASK, 255
  field :SPAN_FLAGS_CONTEXT_HAS_IS_REMOTE_MASK, 256
  field :SPAN_FLAGS_CONTEXT_IS_REMOTE_MASK, 512
end

defmodule Opentelemetry.Proto.Trace.V1.Span.SpanKind do
  @moduledoc false

  use Protobuf, enum: true, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :SPAN_KIND_UNSPECIFIED, 0
  field :SPAN_KIND_INTERNAL, 1
  field :SPAN_KIND_SERVER, 2
  field :SPAN_KIND_CLIENT, 3
  field :SPAN_KIND_PRODUCER, 4
  field :SPAN_KIND_CONSUMER, 5
end

defmodule Opentelemetry.Proto.Trace.V1.Status.StatusCode do
  @moduledoc false

  use Protobuf, enum: true, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :STATUS_CODE_UNSET, 0
  field :STATUS_CODE_OK, 1
  field :STATUS_CODE_ERROR, 2
end

defmodule Opentelemetry.Proto.Trace.V1.TracesData do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :resource_spans, 1,
    repeated: true,
    type: Opentelemetry.Proto.Trace.V1.ResourceSpans,
    json_name: "resourceSpans"
end

defmodule Opentelemetry.Proto.Trace.V1.ResourceSpans do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :resource, 1, type: Opentelemetry.Proto.Resource.V1.Resource

  field :scope_spans, 2,
    repeated: true,
    type: Opentelemetry.Proto.Trace.V1.ScopeSpans,
    json_name: "scopeSpans"

  field :schema_url, 3, type: :string, json_name: "schemaUrl"
end

defmodule Opentelemetry.Proto.Trace.V1.ScopeSpans do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :scope, 1, type: Opentelemetry.Proto.Common.V1.InstrumentationScope
  field :spans, 2, repeated: true, type: Opentelemetry.Proto.Trace.V1.Span
  field :schema_url, 3, type: :string, json_name: "schemaUrl"
end

defmodule Opentelemetry.Proto.Trace.V1.Span.Event do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :time_unix_nano, 1, type: :fixed64, json_name: "timeUnixNano"
  field :name, 2, type: :string
  field :attributes, 3, repeated: true, type: Opentelemetry.Proto.Common.V1.KeyValue
  field :dropped_attributes_count, 4, type: :uint32, json_name: "droppedAttributesCount"
end

defmodule Opentelemetry.Proto.Trace.V1.Span.Link do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :trace_id, 1, type: :bytes, json_name: "traceId"
  field :span_id, 2, type: :bytes, json_name: "spanId"
  field :trace_state, 3, type: :string, json_name: "traceState"
  field :attributes, 4, repeated: true, type: Opentelemetry.Proto.Common.V1.KeyValue
  field :dropped_attributes_count, 5, type: :uint32, json_name: "droppedAttributesCount"
  field :flags, 6, type: :fixed32
end

defmodule Opentelemetry.Proto.Trace.V1.Span do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :trace_id, 1, type: :bytes, json_name: "traceId"
  field :span_id, 2, type: :bytes, json_name: "spanId"
  field :trace_state, 3, type: :string, json_name: "traceState"
  field :parent_span_id, 4, type: :bytes, json_name: "parentSpanId"
  field :flags, 16, type: :fixed32
  field :name, 5, type: :string
  field :kind, 6, type: Opentelemetry.Proto.Trace.V1.Span.SpanKind, enum: true
  field :start_time_unix_nano, 7, type: :fixed64, json_name: "startTimeUnixNano"
  field :end_time_unix_nano, 8, type: :fixed64, json_name: "endTimeUnixNano"
  field :attributes, 9, repeated: true, type: Opentelemetry.Proto.Common.V1.KeyValue
  field :dropped_attributes_count, 10, type: :uint32, json_name: "droppedAttributesCount"
  field :events, 11, repeated: true, type: Opentelemetry.Proto.Trace.V1.Span.Event
  field :dropped_events_count, 12, type: :uint32, json_name: "droppedEventsCount"
  field :links, 13, repeated: true, type: Opentelemetry.Proto.Trace.V1.Span.Link
  field :dropped_links_count, 14, type: :uint32, json_name: "droppedLinksCount"
  field :status, 15, type: Opentelemetry.Proto.Trace.V1.Status
end

defmodule Opentelemetry.Proto.Trace.V1.Status do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :message, 2, type: :string
  field :code, 3, type: Opentelemetry.Proto.Trace.V1.Status.StatusCode, enum: true
end
