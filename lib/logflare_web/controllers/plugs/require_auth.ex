defmodule LogflareWeb.Plugs.RequireAuth do
  @moduledoc false
  import Plug.Conn
  import Phoenix.Controller
  alias Logflare.SingleTenant

  alias LogflareWeb.Router.Helpers, as: Routes

  use LogflareWeb, :routes

  def init(_opts), do: nil

  def call(conn, _opts) do
    is_single_tenant = SingleTenant.single_tenant?()
    current_email = get_session(conn, :current_email)

    cond do
      is_nil(current_email) and is_single_tenant ->
        conn
        |> put_session(:redirect_to, conn.request_path)
        |> redirect(to: ~p"/auth/login/single_tenant")
        |> halt()

      current_email ->
        referer = get_session(conn, :redirect_to)

        if referer do
          conn
          |> put_session(:redirect_to, nil)
          |> redirect(to: referer)
          |> halt()
        else
          conn
          |> put_last_provider_cookie()
        end

      conn.request_path == "/oauth/authorize" ->
        conn
        |> put_session(:oauth_params, conn.params)
        |> redirect(to: Routes.auth_path(conn, :login))
        |> halt()

      true ->
        referer =
          case conn.query_string do
            "" ->
              conn.request_path

            qs ->
              conn.request_path <> "?" <> qs
          end

        conn
        |> put_flash(:error, "You must be logged in.")
        |> put_session(:redirect_to, referer)
        |> redirect(to: Routes.auth_path(conn, :login))
        |> halt()
    end
  end

  defp put_last_provider_cookie(%{assigns: %{user: _user, team_user: team_user}} = conn)
       when is_struct(team_user) do
    put_resp_cookie(
      conn,
      "_logflare_last_provider",
      team_user.provider,
      max_age: 2_592_000
    )
  end

  defp put_last_provider_cookie(%{assigns: %{user: user}} = conn) do
    put_resp_cookie(
      conn,
      "_logflare_last_provider",
      user.provider,
      max_age: 2_592_000
    )
  end
end
