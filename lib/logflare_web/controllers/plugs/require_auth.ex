defmodule LogflareWeb.Plugs.RequireAuth do
  import Plug.Conn
  import Phoenix.Controller

  alias LogflareWeb.Router.Helpers, as: Routes

  def init(_params) do
  end

  def call(conn, _params) do
    cond do
      conn.assigns[:user] ->
        conn

      conn.request_path == "/oauth/authorize" ->
        conn
        |> put_session(:oauth_params, conn.params)
        |> redirect(to: Routes.auth_path(conn, :login))
        |> halt()

      is_nil(conn.assigns[:user]) ->
        conn
        |> put_flash(:error, "You must be logged in.")
        |> redirect(to: Routes.marketing_path(conn, :index))
        |> halt()
    end
  end
end
