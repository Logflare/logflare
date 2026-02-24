defmodule LogflareGrpc.Metrics.Server do
  @moduledoc """
  GRPC Server for the Logflare Metrics Service
  """

  alias Logflare.Logs.Processor
  alias Logflare.Logs.OtelMetric

  alias Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsServiceResponse
  alias Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsServiceRequest

  use GRPC.Server,
    service: Opentelemetry.Proto.Collector.Metrics.V1.MetricsService.Service,
    compressors: [GRPC.Compressor.Gzip, LogflareGrpc.IdentityCompressor],
    http_transcode: true

  require Logger

  def export(%ExportMetricsServiceRequest{resource_metrics: metrics}, stream) do
    Processor.ingest(metrics, OtelMetric, stream.local.source)
    GRPC.Server.set_trailers(stream, %{"grpc-status" => "0"})
    %ExportMetricsServiceResponse{}
  end
end
