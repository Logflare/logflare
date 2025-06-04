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
    api_key = fetch_api_key!(stream)
    source_id = fetch_source_id!(stream)
    {access_token, user} = identify_requestor!(api_key, ["ingest"])
    source = fetch_source!(user, source_id)

    if VerifyResourceAccess.check_resource(source, access_token) do
      next.(rpc_req, %{stream | local: %{source: source}})
    else
      raise GRPC.RPCError, status: :permission_denied
    end
  end

  defp identify_requestor!(api_key, scopes) do
    case VerifyApiAccess.identify_requestor(api_key, scopes) do
      {:ok, access_token, user} ->
        {access_token, user}

      {:error, _reason} ->
        raise GRPC.RPCError, status: :unauthenticated, message: "Invalid API key"
    end
  end

  defp fetch_api_key!(stream) do
    case GRPC.Stream.get_headers(stream) do
      %{"authorization" => "Bearer " <> token} ->
        token

      %{"x-api-key" => key} ->
        key

      _ ->
        raise GRPC.RPCError,
          status: :unauthenticated,
          message: "Missing or invalid API key"
    end
  end

  defp fetch_source_id!(stream) do
    case GRPC.Stream.get_headers(stream) do
      %{"x-collection" => token} -> token
      %{"x-source" => token} -> token
      _ -> raise GRPC.RPCError, status: :unauthenticated, message: "Missing or invalid source id"
    end
  end

  defp fetch_source!(user, source_token) do
    with true <- Sources.valid_source_token_param?(source_token),
         source = %Source{} <- Sources.get_by_and_preload(user_id: user.id, token: source_token) do
      source
    else
      _ -> raise GRPC.RPCError, status: :unauthenticated, message: "Invalid source id"
    end
  end
end
