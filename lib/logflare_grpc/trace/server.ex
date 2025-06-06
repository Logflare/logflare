defmodule LogflareGrpc.Trace.Server do
  @moduledoc """
  GRPC Server for the Logflare Trace Service
  """

  alias Logflare.Logs.Processor
  alias Logflare.Logs.OtelTrace

  alias Opentelemetry.Proto.Collector.Trace.V1.ExportTraceServiceResponse
  alias Opentelemetry.Proto.Collector.Trace.V1.ExportTraceServiceRequest

  use GRPC.Server,
    service: Opentelemetry.Proto.Collector.Trace.V1.TraceService.Service,
    compressors: [GRPC.Compressor.Gzip, LogflareGrpc.IdentityCompressor],
    http_transcode: true

  require Logger

  def export(%ExportTraceServiceRequest{resource_spans: spans}, stream) do
    Processor.ingest(spans, OtelTrace, stream.local.source)
    GRPC.Server.set_trailers(stream, %{"grpc-status" => "0"})
    %ExportTraceServiceResponse{}
  end
end
