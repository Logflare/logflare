defmodule LogflareGrpc.Trace.Server do
  @moduledoc """
  GRPC Server for the Logflare Trace Service
  """
  alias LogflareGrpc.Trace.Transform
  alias Logflare.Sources
  alias Logflare.Source
  alias Logflare.Users
  alias Logflare.Auth
  alias Logflare.User
  alias Logflare.Sources
  alias Logflare.Backends
  alias Opentelemetry.Proto.Collector.Trace.V1.ExportTraceServiceResponse
  alias Opentelemetry.Proto.Collector.Trace.V1.ExportTraceServiceRequest

  use GRPC.Server,
    service: Opentelemetry.Proto.Collector.Trace.V1.TraceService.Service,
    compressors: [GRPC.Compressor.Gzip]

  require Logger

  def export(%ExportTraceServiceRequest{resource_spans: spans} = request, stream) do
    with {:ok, api_key} <- get_access_token(stream),
         {:ok, source_token} <- get_source_token(stream),
         {:ok, user} <- verify_user(api_key),
         {:ok, source} <- get_source(user, source_token) do
      events = Transform.to_log_events(spans, source)
      Backends.ingest_logs(events, source)
      GRPC.Server.set_trailers(stream, %{"grpc-status" => "0"})
      %ExportTraceServiceResponse{}
    else
      {:error, :unauthorized} ->
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
      %{"x-source-token" => token} -> {:ok, token}
      %{"x-source-id" => token} -> {:ok, token}
      _ -> {:error, :unauthorized}
    end
  end

  defp verify_user(access_token_or_api_key) do
    with {:ok, %User{} = owner} <-
           Auth.Cache.verify_access_token(access_token_or_api_key, ["public"]) do
      {:ok, owner}
    else
      {:error, :no_token} = err ->
        err

      {:error, _} = err ->
        if user = Users.Cache.get_by_and_preload(api_key: access_token_or_api_key) do
          {:ok, user}
        else
          {:error, :unauthorized}
        end
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
