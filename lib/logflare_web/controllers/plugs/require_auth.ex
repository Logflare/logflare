defmodule LogflareWeb.Plugs.RequireAuth do
  @moduledoc false
  import Plug.Conn
  import Phoenix.Controller
  alias Logflare.SingleTenant

  alias LogflareWeb.Router.Helpers, as: Routes

  def init(_opts), do: nil

  def call(conn, _opts) do
    assigns = conn.assigns
    user = assigns[:user]
    is_single_tenant = SingleTenant.single_tenant?()
    user_id = get_session(conn, :user_id)

    cond do
      user != nil and is_single_tenant ->
        conn
        |> put_session(:user_id, user.id)

      user && user_id ->
        referer = get_session(conn, :redirect_to)

        if referer do
          conn
          |> put_session(:user_id, user_id)
          |> put_session(:redirect_to, nil)
          |> maybe_get_put_team_user_session()
          |> redirect(to: referer)
          |> halt()
        else
          conn
          |> put_last_provider_cookie()
          |> put_session(:user_id, user_id)
          |> maybe_get_put_team_user_session()
        end

      conn.request_path == "/oauth/authorize" ->
        conn
        |> put_session(:oauth_params, conn.params)
        |> redirect(to: Routes.auth_path(conn, :login))
        |> halt()

      is_nil(user) ->
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

  defp put_last_provider_cookie(%{assigns: %{user: _user, team_user: team_user}} = conn) do
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

  def maybe_get_put_team_user_session(conn) do
    team_user = conn.assigns[:team_user]

    session_team_user_id = get_session(conn, :team_user_id)

    cond do
      team_user ->
        put_session(conn, :team_user_id, team_user.id)

      session_team_user_id ->
        put_session(conn, :team_user_id, session_team_user_id)

      true ->
        conn
    end
  end
end
