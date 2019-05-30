defmodule LogflareWeb.Plugs.CheckSourceCountApi do
  import Phoenix.Controller
  import Plug.Conn

  def init(_opts) do
  end

  def call(conn, _opts) do
    if length(conn.user.sources) < 101 do
      conn
    else
      message = "You have 100 sources. Delete one first!"

      conn
      |> put_status(403)
      |> put_view(LogflareWeb.LogView)
      |> render("index.json", message: message)
      |> halt()
    end
  end
end
