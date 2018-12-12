defmodule LogtailWeb.Plugs.RequireAuth do
  import Plug.Conn
  import Phoenix.Controller

  alias LogtailWeb.Router.Helpers

  def init(_params) do

  end

  def call(conn, _params) do
    if conn.assigns[:user] do
      conn
    else
      conn
      |> put_flash(:error, "You must be logged in.")
      |> redirect(to: Helpers.source_path(conn, :index))
      |> halt()
    end
  end
end
