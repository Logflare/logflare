defmodule LogflareGrpc.Logs.Server do
  @moduledoc """
  GRPC Server for the Logflare Otel Logs Service
  """

  alias Logflare.Logs.Processor
  alias Logflare.Logs.OtelLog

  alias Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceResponse
  alias Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceRequest

  use GRPC.Server,
    service: Opentelemetry.Proto.Collector.Logs.V1.LogsService.Service,
    compressors: [GRPC.Compressor.Gzip, LogflareGrpc.IdentityCompressor],
    http_transcode: true

  require Logger

  def export(%ExportLogsServiceRequest{resource_logs: logs}, stream) do
    Processor.ingest(logs, OtelLog, stream.local.source)
    GRPC.Server.set_trailers(stream, %{"grpc-status" => "0"})
    %ExportLogsServiceResponse{}
  end
end
