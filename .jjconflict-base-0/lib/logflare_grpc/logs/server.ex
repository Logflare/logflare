defmodule LogflareGrpc.Logs.Server do
  @moduledoc """
  GRPC Server for the Logflare Otel Logs Service
  """

  require Logger

  alias Logflare.Logs.Processor
  alias Logflare.Logs.OtelLog

  alias Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceResponse
  alias Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceRequest

  use GRPC.Server,
    service: Opentelemetry.Proto.Collector.Logs.V1.LogsService.Service,
    compressors: [GRPC.Compressor.Gzip, LogflareGrpc.IdentityCompressor],
    http_transcode: true

  def export(%ExportLogsServiceRequest{resource_logs: logs}, stream) do
    source = stream.local.source

    case Processor.ingest(logs, OtelLog, source) do
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
    %ExportLogsServiceResponse{}
  end
end
