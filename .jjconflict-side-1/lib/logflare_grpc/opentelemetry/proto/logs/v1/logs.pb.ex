defmodule Opentelemetry.Proto.Logs.V1.SeverityNumber do
  @moduledoc false

  use Protobuf, enum: true, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

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

  use Protobuf, enum: true, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :LOG_RECORD_FLAGS_DO_NOT_USE, 0
  field :LOG_RECORD_FLAGS_TRACE_FLAGS_MASK, 255
end

defmodule Opentelemetry.Proto.Logs.V1.LogsData do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :resource_logs, 1,
    repeated: true,
    type: Opentelemetry.Proto.Logs.V1.ResourceLogs,
    json_name: "resourceLogs"
end

defmodule Opentelemetry.Proto.Logs.V1.ResourceLogs do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :resource, 1, type: Opentelemetry.Proto.Resource.V1.Resource

  field :scope_logs, 2,
    repeated: true,
    type: Opentelemetry.Proto.Logs.V1.ScopeLogs,
    json_name: "scopeLogs"

  field :schema_url, 3, type: :string, json_name: "schemaUrl"
end

defmodule Opentelemetry.Proto.Logs.V1.ScopeLogs do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :scope, 1, type: Opentelemetry.Proto.Common.V1.InstrumentationScope

  field :log_records, 2,
    repeated: true,
    type: Opentelemetry.Proto.Logs.V1.LogRecord,
    json_name: "logRecords"

  field :schema_url, 3, type: :string, json_name: "schemaUrl"
end

defmodule Opentelemetry.Proto.Logs.V1.LogRecord do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

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
