defmodule Opentelemetry.Proto.Collector.Trace.V1.ExportTraceServiceRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "ExportTraceServiceRequest",
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

defmodule Opentelemetry.Proto.Collector.Trace.V1.ExportTraceServiceResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "ExportTraceServiceResponse",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "partial_success",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.collector.trace.v1.ExportTracePartialSuccess",
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
    type: Opentelemetry.Proto.Collector.Trace.V1.ExportTracePartialSuccess,
    json_name: "partialSuccess"
end

defmodule Opentelemetry.Proto.Collector.Trace.V1.ExportTracePartialSuccess do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "ExportTracePartialSuccess",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "rejected_spans",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_INT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "rejectedSpans",
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

  field :rejected_spans, 1, type: :int64, json_name: "rejectedSpans"
  field :error_message, 2, type: :string, json_name: "errorMessage"
end

defmodule Opentelemetry.Proto.Collector.Trace.V1.TraceService.Service do
  @moduledoc false

  use GRPC.Service,
    name: "opentelemetry.proto.collector.trace.v1.TraceService",
    protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.ServiceDescriptorProto{
      name: "TraceService",
      method: [
        %Google.Protobuf.MethodDescriptorProto{
          name: "Export",
          input_type: ".opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest",
          output_type: ".opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse",
          options: %Google.Protobuf.MethodOptions{
            deprecated: false,
            idempotency_level: :IDEMPOTENCY_UNKNOWN,
            uninterpreted_option: [],
            __pb_extensions__: %{},
            __unknown_fields__: [
              {72_295_728, 2, <<34, 10, 47, 118, 49, 47, 116, 114, 97, 99, 101, 115, 58, 1, 42>>}
            ]
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
      Opentelemetry.Proto.Collector.Trace.V1.ExportTraceServiceRequest,
      Opentelemetry.Proto.Collector.Trace.V1.ExportTraceServiceResponse
end

defmodule Opentelemetry.Proto.Collector.Trace.V1.TraceService.Stub do
  @moduledoc false

  use GRPC.Stub, service: Opentelemetry.Proto.Collector.Trace.V1.TraceService.Service
end