defmodule LogflareGrpc.Trace.Server do
  @moduledoc """
  GRPC Server for the Logflare Trace Service
  """
  alias LogflareGrpc.Trace.Transform
  alias Logflare.Sources
  alias Logflare.Users
  alias Opentelemetry.Proto.Collector.Trace.V1.ExportTraceServiceResponse

  use GRPC.Server,
    service: Opentelemetry.Proto.Collector.Trace.V1.TraceService.Service,
    compressors: [GRPC.Compressor.Gzip]

  require Logger

  def export(%{resource_spans: resource_spans}, stream) do
    with %{"x-api-key" => api_key, "x-source-id" => source_token} <-
           GRPC.Stream.get_headers(stream),
         user when not is_nil(user) <- Users.get_by(api_key: api_key),
         {:ok, source_token} <- Ecto.UUID.cast(source_token),
         source when not is_nil(source) <- Sources.get_by(user_id: user.id, token: source_token),
         log_events <- Transform.to_log_events(resource_spans, source) do
      Enum.each(log_events, fn le -> Logflare.Logs.ingest(le) end)
      ExportTraceServiceResponse.new()
    else
      error ->
        Logger.error("Invalid GRPC request: #{inspect(error)}")
        raise GRPC.RPCError, status: :unauthenticated, message: "Invalid API Key or Source ID"
    end
  end
end
