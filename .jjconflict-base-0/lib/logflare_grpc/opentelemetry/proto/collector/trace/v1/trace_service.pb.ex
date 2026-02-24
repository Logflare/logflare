defmodule Opentelemetry.Proto.Collector.Trace.V1.ExportTraceServiceRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :resource_spans, 1,
    repeated: true,
    type: Opentelemetry.Proto.Trace.V1.ResourceSpans,
    json_name: "resourceSpans"
end

defmodule Opentelemetry.Proto.Collector.Trace.V1.ExportTraceServiceResponse do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :partial_success, 1,
    type: Opentelemetry.Proto.Collector.Trace.V1.ExportTracePartialSuccess,
    json_name: "partialSuccess"
end

defmodule Opentelemetry.Proto.Collector.Trace.V1.ExportTracePartialSuccess do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :rejected_spans, 1, type: :int64, json_name: "rejectedSpans"
  field :error_message, 2, type: :string, json_name: "errorMessage"
end

defmodule Opentelemetry.Proto.Collector.Trace.V1.TraceService.Service do
  @moduledoc false

  use GRPC.Service,
    name: "opentelemetry.proto.collector.trace.v1.TraceService",
    protoc_gen_elixir_version: "0.14.1"

  rpc(
    :Export,
    Opentelemetry.Proto.Collector.Trace.V1.ExportTraceServiceRequest,
    Opentelemetry.Proto.Collector.Trace.V1.ExportTraceServiceResponse
  )
end

defmodule Opentelemetry.Proto.Collector.Trace.V1.TraceService.Stub do
  @moduledoc false

  use GRPC.Stub, service: Opentelemetry.Proto.Collector.Trace.V1.TraceService.Service
end
