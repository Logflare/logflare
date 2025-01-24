defmodule Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsServiceRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "ExportMetricsServiceRequest",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "resource_metrics",
          extendee: nil,
          number: 1,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.metrics.v1.ResourceMetrics",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "resourceMetrics",
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

  field :resource_metrics, 1,
    repeated: true,
    type: Opentelemetry.Proto.Metrics.V1.ResourceMetrics,
    json_name: "resourceMetrics"
end

defmodule Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsServiceResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "ExportMetricsServiceResponse",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "partial_success",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.collector.metrics.v1.ExportMetricsPartialSuccess",
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
    type: Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsPartialSuccess,
    json_name: "partialSuccess"
end

defmodule Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsPartialSuccess do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "ExportMetricsPartialSuccess",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "rejected_data_points",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_INT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "rejectedDataPoints",
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

  field :rejected_data_points, 1, type: :int64, json_name: "rejectedDataPoints"
  field :error_message, 2, type: :string, json_name: "errorMessage"
end

defmodule Opentelemetry.Proto.Collector.Metrics.V1.MetricsService.Service do
  @moduledoc false

  use GRPC.Service,
    name: "opentelemetry.proto.collector.metrics.v1.MetricsService",
    protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.ServiceDescriptorProto{
      name: "MetricsService",
      method: [
        %Google.Protobuf.MethodDescriptorProto{
          name: "Export",
          input_type: ".opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest",
          output_type: ".opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse",
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
      Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsServiceRequest,
      Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsServiceResponse
end

defmodule Opentelemetry.Proto.Collector.Metrics.V1.MetricsService.Stub do
  @moduledoc false

  use GRPC.Stub, service: Opentelemetry.Proto.Collector.Metrics.V1.MetricsService.Service
end