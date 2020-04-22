defmodule LogflareWeb.Auth.OauthProviderController do
  use LogflareWeb, :controller

  alias ExOauth2Provider.Token

  @config Application.get_env(:logflare, ExOauth2Provider)

  def vercel_grant(conn, params) do
    case Token.grant(params, @config) do
      {:ok, access_token} ->
        conn
        |> json(access_token)

      {:error, error, http_status} ->
        conn
        |> put_status(http_status)
        |> json(error)
    end
  end

  def cloudflare_grant(conn, params) do
    case Token.grant(params, @config) do
      {:ok, access_token} ->
        conn
        |> json(Map.drop(access_token, [:expires_in, :refresh_token]))

      {:error, error, http_status} ->
        conn
        |> put_status(http_status)
        |> json(error)
    end
  end
end
