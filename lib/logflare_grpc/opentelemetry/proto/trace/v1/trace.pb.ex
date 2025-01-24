defmodule Opentelemetry.Proto.Trace.V1.SpanFlags do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.EnumDescriptorProto{
      name: "SpanFlags",
      value: [
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SPAN_FLAGS_DO_NOT_USE",
          number: 0,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SPAN_FLAGS_TRACE_FLAGS_MASK",
          number: 255,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SPAN_FLAGS_CONTEXT_HAS_IS_REMOTE_MASK",
          number: 256,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SPAN_FLAGS_CONTEXT_IS_REMOTE_MASK",
          number: 512,
          options: nil,
          __unknown_fields__: []
        }
      ],
      options: nil,
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field :SPAN_FLAGS_DO_NOT_USE, 0
  field :SPAN_FLAGS_TRACE_FLAGS_MASK, 255
  field :SPAN_FLAGS_CONTEXT_HAS_IS_REMOTE_MASK, 256
  field :SPAN_FLAGS_CONTEXT_IS_REMOTE_MASK, 512
end

defmodule Opentelemetry.Proto.Trace.V1.Span.SpanKind do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.EnumDescriptorProto{
      name: "SpanKind",
      value: [
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SPAN_KIND_UNSPECIFIED",
          number: 0,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SPAN_KIND_INTERNAL",
          number: 1,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SPAN_KIND_SERVER",
          number: 2,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SPAN_KIND_CLIENT",
          number: 3,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SPAN_KIND_PRODUCER",
          number: 4,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SPAN_KIND_CONSUMER",
          number: 5,
          options: nil,
          __unknown_fields__: []
        }
      ],
      options: nil,
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field :SPAN_KIND_UNSPECIFIED, 0
  field :SPAN_KIND_INTERNAL, 1
  field :SPAN_KIND_SERVER, 2
  field :SPAN_KIND_CLIENT, 3
  field :SPAN_KIND_PRODUCER, 4
  field :SPAN_KIND_CONSUMER, 5
end

defmodule Opentelemetry.Proto.Trace.V1.Status.StatusCode do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.EnumDescriptorProto{
      name: "StatusCode",
      value: [
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "STATUS_CODE_UNSET",
          number: 0,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "STATUS_CODE_OK",
          number: 1,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "STATUS_CODE_ERROR",
          number: 2,
          options: nil,
          __unknown_fields__: []
        }
      ],
      options: nil,
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field :STATUS_CODE_UNSET, 0
  field :STATUS_CODE_OK, 1
  field :STATUS_CODE_ERROR, 2
end

defmodule Opentelemetry.Proto.Trace.V1.TracesData do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "TracesData",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "resource_spans",
          extendee: nil,
          number: 1,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.trace.v1.ResourceSpans",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "resourceSpans",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field :resource_spans, 1,
    repeated: true,
    type: Opentelemetry.Proto.Trace.V1.ResourceSpans,
    json_name: "resourceSpans"
end

defmodule Opentelemetry.Proto.Trace.V1.ResourceSpans do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "ResourceSpans",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "resource",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.resource.v1.Resource",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "resource",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "scope_spans",
          extendee: nil,
          number: 2,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.trace.v1.ScopeSpans",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "scopeSpans",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "schema_url",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "schemaUrl",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [],
      reserved_range: [
        %Google.Protobuf.DescriptorProto.ReservedRange{
          start: 1000,
          end: 1001,
          __unknown_fields__: []
        }
      ],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field :resource, 1, type: Opentelemetry.Proto.Resource.V1.Resource

  field :scope_spans, 2,
    repeated: true,
    type: Opentelemetry.Proto.Trace.V1.ScopeSpans,
    json_name: "scopeSpans"

  field :schema_url, 3, type: :string, json_name: "schemaUrl"
end

defmodule Opentelemetry.Proto.Trace.V1.ScopeSpans do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "ScopeSpans",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "scope",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.common.v1.InstrumentationScope",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "scope",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "spans",
          extendee: nil,
          number: 2,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.trace.v1.Span",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "spans",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "schema_url",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "schemaUrl",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field :scope, 1, type: Opentelemetry.Proto.Common.V1.InstrumentationScope
  field :spans, 2, repeated: true, type: Opentelemetry.Proto.Trace.V1.Span
  field :schema_url, 3, type: :string, json_name: "schemaUrl"
end

defmodule Opentelemetry.Proto.Trace.V1.Span.Event do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "Event",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "time_unix_nano",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_FIXED64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "timeUnixNano",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "name",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "name",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "attributes",
          extendee: nil,
          number: 3,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.common.v1.KeyValue",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "attributes",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "dropped_attributes_count",
          extendee: nil,
          number: 4,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "droppedAttributesCount",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field :time_unix_nano, 1, type: :fixed64, json_name: "timeUnixNano"
  field :name, 2, type: :string
  field :attributes, 3, repeated: true, type: Opentelemetry.Proto.Common.V1.KeyValue
  field :dropped_attributes_count, 4, type: :uint32, json_name: "droppedAttributesCount"
end

defmodule Opentelemetry.Proto.Trace.V1.Span.Link do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "Link",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "trace_id",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BYTES,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "traceId",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "span_id",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BYTES,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "spanId",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "trace_state",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "traceState",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "attributes",
          extendee: nil,
          number: 4,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.common.v1.KeyValue",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "attributes",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "dropped_attributes_count",
          extendee: nil,
          number: 5,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "droppedAttributesCount",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "flags",
          extendee: nil,
          number: 6,
          label: :LABEL_OPTIONAL,
          type: :TYPE_FIXED32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "flags",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field :trace_id, 1, type: :bytes, json_name: "traceId"
  field :span_id, 2, type: :bytes, json_name: "spanId"
  field :trace_state, 3, type: :string, json_name: "traceState"
  field :attributes, 4, repeated: true, type: Opentelemetry.Proto.Common.V1.KeyValue
  field :dropped_attributes_count, 5, type: :uint32, json_name: "droppedAttributesCount"
  field :flags, 6, type: :fixed32
end

defmodule Opentelemetry.Proto.Trace.V1.Span do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "Span",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "trace_id",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BYTES,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "traceId",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "span_id",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BYTES,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "spanId",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "trace_state",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "traceState",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "parent_span_id",
          extendee: nil,
          number: 4,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BYTES,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "parentSpanId",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "flags",
          extendee: nil,
          number: 16,
          label: :LABEL_OPTIONAL,
          type: :TYPE_FIXED32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "flags",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "name",
          extendee: nil,
          number: 5,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "name",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "kind",
          extendee: nil,
          number: 6,
          label: :LABEL_OPTIONAL,
          type: :TYPE_ENUM,
          type_name: ".opentelemetry.proto.trace.v1.Span.SpanKind",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "kind",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "start_time_unix_nano",
          extendee: nil,
          number: 7,
          label: :LABEL_OPTIONAL,
          type: :TYPE_FIXED64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "startTimeUnixNano",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "end_time_unix_nano",
          extendee: nil,
          number: 8,
          label: :LABEL_OPTIONAL,
          type: :TYPE_FIXED64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "endTimeUnixNano",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "attributes",
          extendee: nil,
          number: 9,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.common.v1.KeyValue",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "attributes",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "dropped_attributes_count",
          extendee: nil,
          number: 10,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "droppedAttributesCount",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "events",
          extendee: nil,
          number: 11,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.trace.v1.Span.Event",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "events",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "dropped_events_count",
          extendee: nil,
          number: 12,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "droppedEventsCount",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "links",
          extendee: nil,
          number: 13,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.trace.v1.Span.Link",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "links",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "dropped_links_count",
          extendee: nil,
          number: 14,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "droppedLinksCount",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "status",
          extendee: nil,
          number: 15,
          label: :LABEL_OPTIONAL,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.trace.v1.Status",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "status",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [
        %Google.Protobuf.DescriptorProto{
          name: "Event",
          field: [
            %Google.Protobuf.FieldDescriptorProto{
              name: "time_unix_nano",
              extendee: nil,
              number: 1,
              label: :LABEL_OPTIONAL,
              type: :TYPE_FIXED64,
              type_name: nil,
              default_value: nil,
              options: nil,
              oneof_index: nil,
              json_name: "timeUnixNano",
              proto3_optional: nil,
              __unknown_fields__: []
            },
            %Google.Protobuf.FieldDescriptorProto{
              name: "name",
              extendee: nil,
              number: 2,
              label: :LABEL_OPTIONAL,
              type: :TYPE_STRING,
              type_name: nil,
              default_value: nil,
              options: nil,
              oneof_index: nil,
              json_name: "name",
              proto3_optional: nil,
              __unknown_fields__: []
            },
            %Google.Protobuf.FieldDescriptorProto{
              name: "attributes",
              extendee: nil,
              number: 3,
              label: :LABEL_REPEATED,
              type: :TYPE_MESSAGE,
              type_name: ".opentelemetry.proto.common.v1.KeyValue",
              default_value: nil,
              options: nil,
              oneof_index: nil,
              json_name: "attributes",
              proto3_optional: nil,
              __unknown_fields__: []
            },
            %Google.Protobuf.FieldDescriptorProto{
              name: "dropped_attributes_count",
              extendee: nil,
              number: 4,
              label: :LABEL_OPTIONAL,
              type: :TYPE_UINT32,
              type_name: nil,
              default_value: nil,
              options: nil,
              oneof_index: nil,
              json_name: "droppedAttributesCount",
              proto3_optional: nil,
              __unknown_fields__: []
            }
          ],
          nested_type: [],
          enum_type: [],
          extension_range: [],
          extension: [],
          options: nil,
          oneof_decl: [],
          reserved_range: [],
          reserved_name: [],
          __unknown_fields__: []
        },
        %Google.Protobuf.DescriptorProto{
          name: "Link",
          field: [
            %Google.Protobuf.FieldDescriptorProto{
              name: "trace_id",
              extendee: nil,
              number: 1,
              label: :LABEL_OPTIONAL,
              type: :TYPE_BYTES,
              type_name: nil,
              default_value: nil,
              options: nil,
              oneof_index: nil,
              json_name: "traceId",
              proto3_optional: nil,
              __unknown_fields__: []
            },
            %Google.Protobuf.FieldDescriptorProto{
              name: "span_id",
              extendee: nil,
              number: 2,
              label: :LABEL_OPTIONAL,
              type: :TYPE_BYTES,
              type_name: nil,
              default_value: nil,
              options: nil,
              oneof_index: nil,
              json_name: "spanId",
              proto3_optional: nil,
              __unknown_fields__: []
            },
            %Google.Protobuf.FieldDescriptorProto{
              name: "trace_state",
              extendee: nil,
              number: 3,
              label: :LABEL_OPTIONAL,
              type: :TYPE_STRING,
              type_name: nil,
              default_value: nil,
              options: nil,
              oneof_index: nil,
              json_name: "traceState",
              proto3_optional: nil,
              __unknown_fields__: []
            },
            %Google.Protobuf.FieldDescriptorProto{
              name: "attributes",
              extendee: nil,
              number: 4,
              label: :LABEL_REPEATED,
              type: :TYPE_MESSAGE,
              type_name: ".opentelemetry.proto.common.v1.KeyValue",
              default_value: nil,
              options: nil,
              oneof_index: nil,
              json_name: "attributes",
              proto3_optional: nil,
              __unknown_fields__: []
            },
            %Google.Protobuf.FieldDescriptorProto{
              name: "dropped_attributes_count",
              extendee: nil,
              number: 5,
              label: :LABEL_OPTIONAL,
              type: :TYPE_UINT32,
              type_name: nil,
              default_value: nil,
              options: nil,
              oneof_index: nil,
              json_name: "droppedAttributesCount",
              proto3_optional: nil,
              __unknown_fields__: []
            },
            %Google.Protobuf.FieldDescriptorProto{
              name: "flags",
              extendee: nil,
              number: 6,
              label: :LABEL_OPTIONAL,
              type: :TYPE_FIXED32,
              type_name: nil,
              default_value: nil,
              options: nil,
              oneof_index: nil,
              json_name: "flags",
              proto3_optional: nil,
              __unknown_fields__: []
            }
          ],
          nested_type: [],
          enum_type: [],
          extension_range: [],
          extension: [],
          options: nil,
          oneof_decl: [],
          reserved_range: [],
          reserved_name: [],
          __unknown_fields__: []
        }
      ],
      enum_type: [
        %Google.Protobuf.EnumDescriptorProto{
          name: "SpanKind",
          value: [
            %Google.Protobuf.EnumValueDescriptorProto{
              name: "SPAN_KIND_UNSPECIFIED",
              number: 0,
              options: nil,
              __unknown_fields__: []
            },
            %Google.Protobuf.EnumValueDescriptorProto{
              name: "SPAN_KIND_INTERNAL",
              number: 1,
              options: nil,
              __unknown_fields__: []
            },
            %Google.Protobuf.EnumValueDescriptorProto{
              name: "SPAN_KIND_SERVER",
              number: 2,
              options: nil,
              __unknown_fields__: []
            },
            %Google.Protobuf.EnumValueDescriptorProto{
              name: "SPAN_KIND_CLIENT",
              number: 3,
              options: nil,
              __unknown_fields__: []
            },
            %Google.Protobuf.EnumValueDescriptorProto{
              name: "SPAN_KIND_PRODUCER",
              number: 4,
              options: nil,
              __unknown_fields__: []
            },
            %Google.Protobuf.EnumValueDescriptorProto{
              name: "SPAN_KIND_CONSUMER",
              number: 5,
              options: nil,
              __unknown_fields__: []
            }
          ],
          options: nil,
          reserved_range: [],
          reserved_name: [],
          __unknown_fields__: []
        }
      ],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

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

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "Status",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "message",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "message",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "code",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_ENUM,
          type_name: ".opentelemetry.proto.trace.v1.Status.StatusCode",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "code",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [
        %Google.Protobuf.EnumDescriptorProto{
          name: "StatusCode",
          value: [
            %Google.Protobuf.EnumValueDescriptorProto{
              name: "STATUS_CODE_UNSET",
              number: 0,
              options: nil,
              __unknown_fields__: []
            },
            %Google.Protobuf.EnumValueDescriptorProto{
              name: "STATUS_CODE_OK",
              number: 1,
              options: nil,
              __unknown_fields__: []
            },
            %Google.Protobuf.EnumValueDescriptorProto{
              name: "STATUS_CODE_ERROR",
              number: 2,
              options: nil,
              __unknown_fields__: []
            }
          ],
          options: nil,
          reserved_range: [],
          reserved_name: [],
          __unknown_fields__: []
        }
      ],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [],
      reserved_range: [
        %Google.Protobuf.DescriptorProto.ReservedRange{start: 1, end: 2, __unknown_fields__: []}
      ],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field :message, 2, type: :string
  field :code, 3, type: Opentelemetry.Proto.Trace.V1.Status.StatusCode, enum: true
end
