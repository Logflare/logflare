defmodule LogflareGrpc.Metrics.Server do
  @moduledoc """
  GRPC Server for the Logflare Metrics Service
  """

  require Logger

  alias Logflare.Logs.Processor
  alias Logflare.Logs.OtelMetric

  alias Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsServiceResponse
  alias Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsServiceRequest

  use GRPC.Server,
    service: Opentelemetry.Proto.Collector.Metrics.V1.MetricsService.Service,
    compressors: [GRPC.Compressor.Gzip, LogflareGrpc.IdentityCompressor],
    http_transcode: true

  def export(%ExportMetricsServiceRequest{resource_metrics: metrics}, stream) do
    source = stream.local.source

    case Processor.ingest(metrics, OtelMetric, source) do
      {:error, errors} when is_list(errors) ->
        Logger.warning("OTLP gRPC ingest rejected #{length(errors)} event(s) at validation",
          source_token: source.token,
          source_id: source.id,
          sample_errors: errors |> Enum.uniq() |> Enum.take(3)
        )

      _ ->
        :ok
    end

    GRPC.Server.set_trailers(stream, %{"grpc-status" => "0"})
    %ExportMetricsServiceResponse{}
  end
end
