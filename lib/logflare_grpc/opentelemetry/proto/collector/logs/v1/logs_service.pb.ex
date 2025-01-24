defmodule Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "ExportLogsServiceRequest",
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

defmodule Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "ExportLogsServiceResponse",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "partial_success",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.collector.logs.v1.ExportLogsPartialSuccess",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "partialSuccess",
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

  field :partial_success, 1,
    type: Opentelemetry.Proto.Collector.Logs.V1.ExportLogsPartialSuccess,
    json_name: "partialSuccess"
end

defmodule Opentelemetry.Proto.Collector.Logs.V1.ExportLogsPartialSuccess do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "ExportLogsPartialSuccess",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "rejected_log_records",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_INT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "rejectedLogRecords",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "error_message",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "errorMessage",
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

  field :rejected_log_records, 1, type: :int64, json_name: "rejectedLogRecords"
  field :error_message, 2, type: :string, json_name: "errorMessage"
end

defmodule Opentelemetry.Proto.Collector.Logs.V1.LogsService.Service do
  @moduledoc false

  use GRPC.Service,
    name: "opentelemetry.proto.collector.logs.v1.LogsService",
    protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.ServiceDescriptorProto{
      name: "LogsService",
      method: [
        %Google.Protobuf.MethodDescriptorProto{
          name: "Export",
          input_type: ".opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest",
          output_type: ".opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse",
          options: %Google.Protobuf.MethodOptions{
            deprecated: false,
            idempotency_level: :IDEMPOTENCY_UNKNOWN,
            uninterpreted_option: [],
            __pb_extensions__: %{},
            __unknown_fields__: []
          },
          client_streaming: false,
          server_streaming: false,
          __unknown_fields__: []
        }
      ],
      options: nil,
      __unknown_fields__: []
    }
  end

  rpc :Export,
      Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceRequest,
      Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceResponse
end

defmodule Opentelemetry.Proto.Collector.Logs.V1.LogsService.Stub do
  @moduledoc false

  use GRPC.Stub, service: Opentelemetry.Proto.Collector.Logs.V1.LogsService.Service
end