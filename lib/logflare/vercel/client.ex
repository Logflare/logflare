defmodule Logflare.Vercel.Client do
  require Logger

  @client_id Application.get_env(:logflare, __MODULE__)[:client_id]
  @client_secret Application.get_env(:logflare, __MODULE__)[:client_secret]
  @redirect_uri Application.get_env(:logflare, __MODULE__)[:redirect_uri]

  def new(access_token \\ nil) do
    middleware = [
      {Tesla.Middleware.BaseUrl, "https://api.vercel.com"},
      Tesla.Middleware.JSON,
      Tesla.Middleware.FormUrlencoded,
      {Tesla.Middleware.Headers, headers(access_token)}
    ]

    adapter = {Tesla.Adapter.Mint, timeout: 60_000, mode: :passive}

    Tesla.client(middleware, adapter)
  end

  def get_access_token(client, code) do
    body = %{
      client_id: @client_id,
      client_secret: @client_secret,
      code: code,
      redirect_uri: @redirect_uri
    }

    url = "/v2/oauth/access_token"

    client
    |> Tesla.post(url, body)
  end

  def list_log_drains(client) do
    client
    |> Tesla.get("/v1/integrations/log-drains")
  end

  def create_log_drain(client, params) when is_map(params) do
    client
    |> Tesla.post("/v1/integrations/log-drains", params)
  end

  def delete_log_drain(client, drain_id) when is_binary(drain_id) do
    client
    |> Tesla.delete("/v1/integrations/log-drains/" <> drain_id)
  end

  defp headers(nil) do
    []
  end

  defp headers(access_token) when is_binary(access_token) do
    [{"authorization", "Bearer " <> access_token}]
  end
end
