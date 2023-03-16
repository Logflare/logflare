defmodule LogflareWeb.Plugs.VerifyApiAccess do
  @moduledoc """
  Verifies if a user has access to a requested resource.

  Assigns the token's associated user if the token is provided

  Authentication api key can either be through access tokens or legacy `user.api_key`.

  Access token usage is preferred and `user.api_key` is only used as a fallback.
  """
  import Plug.Conn
  alias Logflare.Auth
  alias Logflare.Endpoints.Query
  alias Logflare.Users
  alias LogflareWeb.Api.FallbackController

  def init(args), do: args |> Enum.into(%{})

  def call(%{assigns: %{endpoint: %Query{enable_auth: false}}} = conn, _opts), do: conn

  def call(conn, opts) do
    opts = Enum.into(opts, %{scopes: []})
    # generic access
    with {:ok, user} <- identify_requestor(conn, opts.scopes) do
      assign(conn, :user, user)
    else
      _ ->
        FallbackController.call(conn, {:error, :unauthorized})
    end
  end

  defp identify_requestor(conn, scopes) do
    extracted = extract_token(conn)
    is_private_route? = "private" in scopes

    with {:ok, access_token_or_api_key} <- extracted,
         {:ok, user} <- Auth.verify_access_token(access_token_or_api_key, scopes) do
      {:ok, user}
    else
      {:error, :no_token} = err ->
        err

      {:error, _} = err ->
        handle_legacy_api_key(extracted, err, is_private_route?)
    end
  end

  defp handle_legacy_api_key({:ok, api_key}, err, is_private_route?) do
    case Users.get_by(api_key: api_key) do
      %_{} = user when is_private_route? == false -> {:ok, user}
      _ when is_private_route? == true -> {:error, :unauthorized}
      _ -> err
    end
  end

  defp extract_token(conn) do
    auth_header =
      conn.req_headers
      |> Enum.into(%{})
      |> Map.get("authorization")

    bearer =
      if auth_header && String.contains?(auth_header, "Bearer ") do
        String.split(auth_header, " ")
        |> Enum.at(1)
      end

    api_key =
      conn.req_headers
      |> Enum.into(%{})
      |> Map.get("x-api-key", conn.params["api_key"])

    cond do
      bearer != nil -> {:ok, bearer}
      api_key != nil -> {:ok, api_key}
      true -> {:error, :no_token}
    end
  end
end
