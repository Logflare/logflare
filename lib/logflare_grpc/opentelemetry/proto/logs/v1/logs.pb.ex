defmodule Opentelemetry.Proto.Logs.V1.SeverityNumber do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.EnumDescriptorProto{
      name: "SeverityNumber",
      value: [
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SEVERITY_NUMBER_UNSPECIFIED",
          number: 0,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SEVERITY_NUMBER_TRACE",
          number: 1,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SEVERITY_NUMBER_TRACE2",
          number: 2,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SEVERITY_NUMBER_TRACE3",
          number: 3,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SEVERITY_NUMBER_TRACE4",
          number: 4,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SEVERITY_NUMBER_DEBUG",
          number: 5,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SEVERITY_NUMBER_DEBUG2",
          number: 6,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SEVERITY_NUMBER_DEBUG3",
          number: 7,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SEVERITY_NUMBER_DEBUG4",
          number: 8,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SEVERITY_NUMBER_INFO",
          number: 9,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SEVERITY_NUMBER_INFO2",
          number: 10,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SEVERITY_NUMBER_INFO3",
          number: 11,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SEVERITY_NUMBER_INFO4",
          number: 12,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SEVERITY_NUMBER_WARN",
          number: 13,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SEVERITY_NUMBER_WARN2",
          number: 14,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SEVERITY_NUMBER_WARN3",
          number: 15,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SEVERITY_NUMBER_WARN4",
          number: 16,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SEVERITY_NUMBER_ERROR",
          number: 17,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SEVERITY_NUMBER_ERROR2",
          number: 18,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SEVERITY_NUMBER_ERROR3",
          number: 19,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SEVERITY_NUMBER_ERROR4",
          number: 20,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SEVERITY_NUMBER_FATAL",
          number: 21,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SEVERITY_NUMBER_FATAL2",
          number: 22,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SEVERITY_NUMBER_FATAL3",
          number: 23,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "SEVERITY_NUMBER_FATAL4",
          number: 24,
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

  field :SEVERITY_NUMBER_UNSPECIFIED, 0
  field :SEVERITY_NUMBER_TRACE, 1
  field :SEVERITY_NUMBER_TRACE2, 2
  field :SEVERITY_NUMBER_TRACE3, 3
  field :SEVERITY_NUMBER_TRACE4, 4
  field :SEVERITY_NUMBER_DEBUG, 5
  field :SEVERITY_NUMBER_DEBUG2, 6
  field :SEVERITY_NUMBER_DEBUG3, 7
  field :SEVERITY_NUMBER_DEBUG4, 8
  field :SEVERITY_NUMBER_INFO, 9
  field :SEVERITY_NUMBER_INFO2, 10
  field :SEVERITY_NUMBER_INFO3, 11
  field :SEVERITY_NUMBER_INFO4, 12
  field :SEVERITY_NUMBER_WARN, 13
  field :SEVERITY_NUMBER_WARN2, 14
  field :SEVERITY_NUMBER_WARN3, 15
  field :SEVERITY_NUMBER_WARN4, 16
  field :SEVERITY_NUMBER_ERROR, 17
  field :SEVERITY_NUMBER_ERROR2, 18
  field :SEVERITY_NUMBER_ERROR3, 19
  field :SEVERITY_NUMBER_ERROR4, 20
  field :SEVERITY_NUMBER_FATAL, 21
  field :SEVERITY_NUMBER_FATAL2, 22
  field :SEVERITY_NUMBER_FATAL3, 23
  field :SEVERITY_NUMBER_FATAL4, 24
end

defmodule Opentelemetry.Proto.Logs.V1.LogRecordFlags do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.EnumDescriptorProto{
      name: "LogRecordFlags",
      value: [
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "LOG_RECORD_FLAGS_DO_NOT_USE",
          number: 0,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "LOG_RECORD_FLAGS_TRACE_FLAGS_MASK",
          number: 255,
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

  field :LOG_RECORD_FLAGS_DO_NOT_USE, 0
  field :LOG_RECORD_FLAGS_TRACE_FLAGS_MASK, 255
end

defmodule Opentelemetry.Proto.Logs.V1.LogsData do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "LogsData",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "resource_logs",
          extendee: nil,
          number: 1,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.logs.v1.ResourceLogs",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "resourceLogs",
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

  field :resource_logs, 1,
    repeated: true,
    type: Opentelemetry.Proto.Logs.V1.ResourceLogs,
    json_name: "resourceLogs"
end

defmodule Opentelemetry.Proto.Logs.V1.ResourceLogs do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "ResourceLogs",
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
          name: "scope_logs",
          extendee: nil,
          number: 2,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.logs.v1.ScopeLogs",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "scopeLogs",
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

  field :scope_logs, 2,
    repeated: true,
    type: Opentelemetry.Proto.Logs.V1.ScopeLogs,
    json_name: "scopeLogs"

  field :schema_url, 3, type: :string, json_name: "schemaUrl"
end

defmodule Opentelemetry.Proto.Logs.V1.ScopeLogs do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "ScopeLogs",
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
          name: "log_records",
          extendee: nil,
          number: 2,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.logs.v1.LogRecord",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "logRecords",
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

  field :log_records, 2,
    repeated: true,
    type: Opentelemetry.Proto.Logs.V1.LogRecord,
    json_name: "logRecords"

  field :schema_url, 3, type: :string, json_name: "schemaUrl"
end

defmodule Opentelemetry.Proto.Logs.V1.LogRecord do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "LogRecord",
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
          name: "observed_time_unix_nano",
          extendee: nil,
          number: 11,
          label: :LABEL_OPTIONAL,
          type: :TYPE_FIXED64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "observedTimeUnixNano",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "severity_number",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_ENUM,
          type_name: ".opentelemetry.proto.logs.v1.SeverityNumber",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "severityNumber",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "severity_text",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "severityText",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "body",
          extendee: nil,
          number: 5,
          label: :LABEL_OPTIONAL,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.common.v1.AnyValue",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "body",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "attributes",
          extendee: nil,
          number: 6,
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
          number: 7,
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
          number: 8,
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
          name: "trace_id",
          extendee: nil,
          number: 9,
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
          number: 10,
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
          name: "event_name",
          extendee: nil,
          number: 12,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "eventName",
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
        %Google.Protobuf.DescriptorProto.ReservedRange{start: 4, end: 5, __unknown_fields__: []}
      ],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field :time_unix_nano, 1, type: :fixed64, json_name: "timeUnixNano"
  field :observed_time_unix_nano, 11, type: :fixed64, json_name: "observedTimeUnixNano"

  field :severity_number, 2,
    type: Opentelemetry.Proto.Logs.V1.SeverityNumber,
    json_name: "severityNumber",
    enum: true

  field :severity_text, 3, type: :string, json_name: "severityText"
  field :body, 5, type: Opentelemetry.Proto.Common.V1.AnyValue
  field :attributes, 6, repeated: true, type: Opentelemetry.Proto.Common.V1.KeyValue
  field :dropped_attributes_count, 7, type: :uint32, json_name: "droppedAttributesCount"
  field :flags, 8, type: :fixed32
  field :trace_id, 9, type: :bytes, json_name: "traceId"
  field :span_id, 10, type: :bytes, json_name: "spanId"
  field :event_name, 12, type: :string, json_name: "eventName"
end