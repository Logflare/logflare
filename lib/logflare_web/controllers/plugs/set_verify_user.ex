defmodule LogflareWeb.Plugs.SetVerifyUser do
  @moduledoc """
  Assigns user if api key or browser session is present in conn
  """
  import Plug.Conn
  import Phoenix.Controller
  alias Logflare.{Users, User}
  alias ExOauth2Provider.AccessTokens

  @oauth_config Application.get_env(:logflare, ExOauth2Provider)

  def init(_), do: nil

  def call(%{assigns: %{user: %User{}}} = conn, _opts), do: conn

  # Deprecate when route is deprecated
  def call(%{request_path: "/api/logs" <> _} = conn, opts),
    do: set_user_for_ingest_api(conn, opts)

  def call(%{request_path: "/logs" <> _} = conn, opts),
    do: set_user_for_ingest_api(conn, opts)

  def call(%{request_path: "/api" <> _} = conn, opts),
    do: set_user_for_mgmt_api(conn, opts)

  def call(conn, opts),
    do: set_user_for_browser(conn, opts)

  defp set_user_for_browser(conn, _opts) do
    user =
      conn
      |> get_session(:user_id)
      |> maybe_parse_binary_to_int()
      |> case do
        id when is_integer(id) ->
          Users.get_by_and_preload(id: id)
          |> Users.preload_team()
          |> Users.preload_billing_account()
          |> Users.preload_sources()

        _ ->
          nil
      end

    assign(conn, :user, user)
  end

  defp set_user_for_mgmt_api(conn, _opts) do
    auth_header =
      conn.req_headers
      |> Enum.into(%{})
      |> Map.get("authorization")

    bearer =
      if auth_header && String.contains?(auth_header, "Bearer ") do
        String.split(auth_header, " ")
        |> Enum.at(1)
      end

    cond do
      is_nil(bearer) ->
        message = "Error: please authenticate"
        put_401(conn, message)

      is_expired?(bearer) ->
        # Old tokens expire. Tokens are now set to never expire.
        # Revisit when mgmt api is made public.
        message = "Error: token expired"
        put_401(conn, message)

      true ->
        oauth_access_token = AccessTokens.get_by_token(bearer, @oauth_config)
        user = Users.Cache.get_by_and_preload(id: oauth_access_token.resource_owner_id)

        assign(conn, :user, user)
    end
  end

  defp set_user_for_ingest_api(conn, _opts) do
    api_key =
      conn.req_headers
      |> Enum.into(%{})
      |> Map.get("x-api-key", conn.params["api_key"])

    case api_key && Users.Cache.get_by_and_preload(api_key: api_key) do
      %User{} = user ->
        assign(conn, :user, user)

      api_key when is_binary(api_key) ->
        message = "Error: please set ingest API key"
        put_401(conn, message)

      nil ->
        message = "Error: user not found"
        put_401(conn, message)
    end
  end

  defp maybe_parse_binary_to_int(nil), do: nil
  defp maybe_parse_binary_to_int(x) when is_integer(x), do: x

  defp maybe_parse_binary_to_int(x) do
    {int, ""} = Integer.parse(x)
    int
  end

  defp put_401(conn, message) do
    conn
    |> put_status(401)
    |> put_view(LogflareWeb.LogView)
    |> render("index.json", message: message)
    |> halt()
  end

  defp is_expired?(bearer) do
    AccessTokens.get_by_token(bearer, @oauth_config)
    |> AccessTokens.is_expired?()
  end
end
