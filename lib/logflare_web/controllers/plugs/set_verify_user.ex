defmodule LogflareWeb.Plugs.SetVerifyUser do
  @moduledoc """
  Assigns user if api key or browser session is present in conn
  """
  import Plug.Conn
  import Phoenix.Controller
  alias Logflare.{Users, User}
  alias ExOauth2Provider.AccessTokens
  alias Logflare.SingleTenant

  defp env_oauth_config, do: Application.get_env(:logflare, ExOauth2Provider)

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
    is_single_tenant = SingleTenant.single_tenant?()

    user =
      conn
      |> get_session(:user_id)
      |> maybe_parse_binary_to_int()
      # handle single tenant browser usage, should have no auth required
      |> case do
        nil when is_single_tenant == true ->
          SingleTenant.get_default_user().id

        other ->
          other
      end
      |> fetch_preloaded_user_by_id()

    assign(conn, :user, user)
  end

  defp set_user_for_mgmt_api(conn, _opts) do
    case get_req_header(conn, "authorization") do
      ["Bearer " <> bearer] ->
        case is_expired?(bearer) do
          true ->
            message = "Error: token expired"
            put_401(conn, message)

          false ->
            oauth_access_token = AccessTokens.get_by_token(bearer, env_oauth_config())
            user = Users.Cache.get_by_and_preload(id: oauth_access_token.resource_owner_id)

            assign(conn, :user, user)
        end

      _ ->
        put_401(conn, "Error: please authenticate")
    end
  end

  defp set_user_for_ingest_api(conn, _opts) do
    api_key =
      case get_req_header(conn, "x-api-key") do
        [] -> conn.params["api_key"]
        [api_key] -> api_key
      end

    case api_key && Users.Cache.get_by_and_preload(api_key: api_key) do
      %User{} = user ->
        assign(conn, :user, user)

      api_key when is_binary(api_key) ->
        message = "Error: ingest api_key not authorized"
        put_401(conn, message)

      nil ->
        message = "Error: ingest api_key required in url parameter or x-api-key request header"
        put_401(conn, message)
    end
  end

  defp maybe_parse_binary_to_int(nil), do: nil
  defp maybe_parse_binary_to_int(x) when is_integer(x), do: x

  defp maybe_parse_binary_to_int(x) do
    {int, ""} = Integer.parse(x)
    int
  end

  defp fetch_preloaded_user_by_id(id) when is_integer(id) do
    Users.get_by_and_preload(id: id)
    |> Users.preload_team()
    |> Users.preload_billing_account()
    |> Users.preload_sources()
  end

  defp fetch_preloaded_user_by_id(_id), do: nil

  defp put_401(conn, message) do
    conn
    |> put_status(401)
    |> put_view(LogflareWeb.LogView)
    |> render("index.json", message: message)
    |> halt()
  end

  defp is_expired?(bearer) do
    bearer
    |> AccessTokens.get_by_token(env_oauth_config())
    |> AccessTokens.is_expired?()
  end
end
