defmodule LogflareWeb.Plugs.RequireAuth do
  import Plug.Conn
  import Phoenix.Controller

  alias LogflareWeb.Router.Helpers, as: Routes

  def init(_opts), do: nil

  def call(conn, _opts) do
    cond do
      user = conn.assigns[:user] ->
        user_id = get_session(conn, :user_id)

        if user_id do
          referer = get_session(conn, :redirect_to)

          if referer do
            conn
            |> put_session(:user_id, user_id)
            |> put_session(:redirect_to, nil)
            |> redirect(to: referer)
            |> halt()
          else
            conn
            |> put_last_provider_cookie()
            |> put_session(:user_id, user_id)
          end
        end

      conn.request_path == "/oauth/authorize" ->
        conn
        |> put_session(:oauth_params, conn.params)
        |> redirect(to: Routes.auth_path(conn, :login))
        |> halt()

      is_nil(conn.assigns[:user]) ->
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
end
