defmodule Logflare.Vercel.Client do
  require Logger

  alias Logflare.Vercel

  @client_id Application.get_env(:logflare, __MODULE__)[:client_id]
  @client_secret Application.get_env(:logflare, __MODULE__)[:client_secret]
  @redirect_uri Application.get_env(:logflare, __MODULE__)[:redirect_uri]

  def new() do
    new(%Vercel.Auth{})
  end

  def new(%Vercel.Auth{} = auth) do
    middleware = [
      {Tesla.Middleware.BaseUrl, "https://api.vercel.com"},
      Tesla.Middleware.FormUrlencoded,
      Tesla.Middleware.JSON,
      {Tesla.Middleware.Headers, headers(auth)},
      {Tesla.Middleware.Query, query_params(auth)}
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

  defp query_params(%Vercel.Auth{team_id: nil}) do
    []
  end

  defp query_params(%Vercel.Auth{team_id: team_id}) when is_binary(team_id) do
    [teamId: team_id]
  end

  defp headers(%Vercel.Auth{access_token: nil}) do
    []
  end

  defp headers(%Vercel.Auth{access_token: access_token}) when is_binary(access_token) do
    [{"authorization", "Bearer " <> access_token}]
  end
end
