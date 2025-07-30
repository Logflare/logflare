defmodule LogflareGrpc.Interceptors.VerifyApiResourceAccess do
  @moduledoc """
  Authentication and authorization interceptor for OTLP gRPC servers

  Assigns `source` to the stream.
  """

  alias Logflare.Source
  alias Logflare.Sources

  alias LogflareWeb.Plugs.VerifyApiAccess
  alias LogflareWeb.Plugs.VerifyResourceAccess

  @behaviour GRPC.Server.Interceptor

  def init(options) do
    options
  end

  def call(rpc_req, stream, next, _options) do
    with {:ok, api_key} <- fetch_api_key(stream),
         {:ok, source_id} <- fetch_source_id(stream),
         {:ok, access_token, user} <- identify_requestor(api_key, []),
         {:ok, source} <- fetch_source(user, source_id) do
      if VerifyResourceAccess.check_resource(source, access_token) do
        next.(rpc_req, %{stream | local: %{source: source}})
      else
        {:error, GRPC.RPCError.exception(status: :permission_denied)}
      end
    end
  end

  defp identify_requestor(api_key, scopes) do
    case VerifyApiAccess.identify_requestor(api_key, scopes) do
      {:ok, access_token, user} ->
        {:ok, access_token, user}

      {:error, _reason} ->
        {:error, GRPC.RPCError.exception(status: :permission_denied)}
    end
  end

  defp fetch_api_key(stream) do
    case GRPC.Stream.get_headers(stream) do
      %{"authorization" => "Bearer " <> token} ->
        {:ok, token}

      %{"x-api-key" => token} ->
        {:ok, token}

      _ ->
        {:error,
         GRPC.RPCError.exception(
           status: :unauthenticated,
           message: "Missing or invalid API key"
         )}
    end
  end

  defp fetch_source_id(stream) do
    case GRPC.Stream.get_headers(stream) do
      %{"x-collection" => source_uuid} ->
        {:ok, source_uuid}

      %{"x-source" => source_uuid} ->
        {:ok, source_uuid}

      _ ->
        {:error,
         GRPC.RPCError.exception(
           status: :unauthenticated,
           message: "Missing source id"
         )}
    end
  end

  defp fetch_source(user, source_token) do
    if Sources.valid_source_token_param?(source_token) do
      case Sources.Cache.get_by_and_preload_rules(user_id: user.id, token: source_token) do
        %Source{} = source ->
          source = Sources.refresh_source_metrics_for_ingest(source)
          {:ok, source}

        _ ->
          {:error, GRPC.RPCError.exception(status: :permission_denied)}
      end
    else
      {:error, GRPC.RPCError.exception(status: :unauthenticated, message: "Invalid source id")}
    end
  end
end
