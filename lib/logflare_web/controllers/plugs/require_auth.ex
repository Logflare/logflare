defmodule LogflareWeb.Plugs.RequireAuth do
  import Plug.Conn
  import Phoenix.Controller

  alias LogflareWeb.Router.Helpers

  def init(_params) do
  end

  def call(conn, _params) do
    cond do
      conn.assigns[:user] ->
        conn

      conn.request_path == "/oauth/authorize" ->
        # put conn.params and the path in the session with put_session(conn, key, value)
        # then when someone comes back from github if they have that in the put_session
        # redirect to the oauth/authorize page with the params

        # conn = put_session(conn, :request_path, request_path)
        scheme = Atom.to_string(conn.scheme)

        oauth_path = scheme <> "://" <> conn.host <> conn.request_path <> "?" <> conn.query_string

        conn
        |> put_session(:oauth_path, oauth_path)
        |> redirect(to: Helpers.auth_path(conn, :request, "github"))
        |> halt()

      is_nil(conn.assigns[:user]) ->
        conn
        |> put_flash(:error, "You must be logged in.")
        |> redirect(to: Helpers.source_path(conn, :index))
        |> halt()
    end
  end
end
