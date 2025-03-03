defmodule LogflareGrpc.Trace.Server do
  @moduledoc """
  GRPC Server for the Logflare Trace Service
  """
  alias Logflare.Logs.Processor
  alias Logflare.Logs.OtelTrace
  alias Logflare.Sources
  alias Logflare.Source
  alias Logflare.Users
  alias Logflare.Sources
  alias LogflareWeb.Plugs.VerifyApiAccess
  alias LogflareWeb.Plugs.VerifyResourceAccess
  alias Opentelemetry.Proto.Collector.Trace.V1.ExportTraceServiceResponse
  alias Opentelemetry.Proto.Collector.Trace.V1.ExportTraceServiceRequest

  use GRPC.Server,
    service: Opentelemetry.Proto.Collector.Trace.V1.TraceService.Service,
    compressors: [GRPC.Compressor.Gzip, LogflareGrpc.IdentityCompressor],
    http_transcode: true

  require Logger

  def export(%ExportTraceServiceRequest{resource_spans: spans}, stream) do
    with {:ok, api_key} <- get_access_token(stream),
         {:ok, source_token} <- get_source_token(stream),
         {:ok, access_token, user} <- VerifyApiAccess.identify_requestor(api_key, ["ingest"]),
         {:ok, source} <- get_source(user, source_token),
         {:scopes, true} <- {:scopes, VerifyResourceAccess.check_resource(source, access_token)} do
      Processor.ingest(spans, OtelTrace, source)

      GRPC.Server.set_trailers(stream, %{"grpc-status" => "0"})
      %ExportTraceServiceResponse{}
    else
      e
      when e in [
             {:error, :unauthorized},
             {:scopes, false},
             {:error, :no_token}
           ] ->
        raise GRPC.RPCError, status: :unauthenticated, message: "Invalid API Key or Source ID"

      err ->
        raise GRPC.RPCError, status: :unknown, message: "Unknown error: #{inspect(err)}"
    end
  end

  defp get_access_token(stream) do
    case GRPC.Stream.get_headers(stream) do
      %{"authorization" => "Bearer " <> token} -> {:ok, token}
      %{"x-api-key" => key} -> {:ok, key}
      _ -> {:error, :unauthorized}
    end
  end

  defp get_source_token(stream) do
    case GRPC.Stream.get_headers(stream) do
      %{"x-collection" => token} -> {:ok, token}
      %{"x-source" => token} -> {:ok, token}
      _ -> {:error, :unauthorized}
    end
  end

  defp get_source(user, source_token) do
    with true <- Sources.valid_source_token_param?(source_token),
         source = %Source{} <- Sources.get_by_and_preload(user_id: user.id, token: source_token) do
      {:ok, source}
    else
      _ ->
        {:error, :unauthorized}
    end
  end
end
