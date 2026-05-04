defmodule LogflareGrpc.Trace.Server do
  @moduledoc """
  GRPC Server for the Logflare Trace Service
  """

  require Logger

  alias Logflare.Logs.Processor
  alias Logflare.Logs.OtelTrace

  alias Opentelemetry.Proto.Collector.Trace.V1.ExportTraceServiceResponse
  alias Opentelemetry.Proto.Collector.Trace.V1.ExportTraceServiceRequest

  use GRPC.Server,
    service: Opentelemetry.Proto.Collector.Trace.V1.TraceService.Service,
    compressors: [GRPC.Compressor.Gzip, LogflareGrpc.IdentityCompressor],
    http_transcode: true

  def export(%ExportTraceServiceRequest{resource_spans: spans}, stream) do
    source = stream.local.source

    case Processor.ingest(spans, OtelTrace, source) do
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
    %ExportTraceServiceResponse{}
  end
end
