defmodule LogflareGrpc.Vector.Server do
  @moduledoc """
  gRPC server for the Vector `vector.Vector` ingestion service.

  Accepts `PushEvents` requests sent by the [Vector](https://vector.dev)
  `vector` sink and ingests `Log`, `Metric` and `Trace` events into a
  Logflare source. The source is resolved from the `x-source` (or
  `x-collection`) gRPC metadata header by
  `LogflareGrpc.Interceptors.VerifyApiResourceAccess`.
  """

  require Logger

  alias Logflare.Logs.Processor
  alias Logflare.Logs.VectorGrpc

  alias Vector.HealthCheckRequest
  alias Vector.HealthCheckResponse
  alias Vector.PushEventsRequest
  alias Vector.PushEventsResponse

  use GRPC.Server,
    service: Vector.Vector.Service,
    compressors: [GRPC.Compressor.Gzip, LogflareGrpc.IdentityCompressor]

  @spec push_events(PushEventsRequest.t(), GRPC.Server.Stream.t()) :: PushEventsResponse.t()
  def push_events(%PushEventsRequest{events: events}, stream) do
    source = stream.local.source

    case Processor.ingest(events || [], VectorGrpc, source) do
      {:error, errors} when is_list(errors) ->
        Logger.warning("Vector gRPC ingest rejected #{length(errors)} event(s) at validation",
          source_token: source.token,
          source_id: source.id,
          sample_errors: errors |> Enum.uniq() |> Enum.take(3)
        )

      _ ->
        :ok
    end

    GRPC.Server.set_trailers(stream, %{"grpc-status" => "0"})
    %PushEventsResponse{}
  end

  @spec health_check(HealthCheckRequest.t(), GRPC.Server.Stream.t()) :: HealthCheckResponse.t()
  def health_check(%HealthCheckRequest{}, _stream) do
    %HealthCheckResponse{status: :SERVING}
  end
end
