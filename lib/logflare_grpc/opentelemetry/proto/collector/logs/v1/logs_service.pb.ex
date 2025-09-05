defmodule Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :resource_logs, 1,
    repeated: true,
    type: Opentelemetry.Proto.Logs.V1.ResourceLogs,
    json_name: "resourceLogs"
end

defmodule Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceResponse do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :partial_success, 1,
    type: Opentelemetry.Proto.Collector.Logs.V1.ExportLogsPartialSuccess,
    json_name: "partialSuccess"
end

defmodule Opentelemetry.Proto.Collector.Logs.V1.ExportLogsPartialSuccess do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :rejected_log_records, 1, type: :int64, json_name: "rejectedLogRecords"
  field :error_message, 2, type: :string, json_name: "errorMessage"
end

defmodule Opentelemetry.Proto.Collector.Logs.V1.LogsService.Service do
  @moduledoc false

  use GRPC.Service,
    name: "opentelemetry.proto.collector.logs.v1.LogsService",
    protoc_gen_elixir_version: "0.14.1"

  rpc(
    :Export,
    Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceRequest,
    Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceResponse
  )
end

defmodule Opentelemetry.Proto.Collector.Logs.V1.LogsService.Stub do
  @moduledoc false

  use GRPC.Stub, service: Opentelemetry.Proto.Collector.Logs.V1.LogsService.Service
end
