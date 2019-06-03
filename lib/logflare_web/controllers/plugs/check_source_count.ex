defmodule LogflareWeb.Plugs.CheckSourceCount do
  @moduledoc false
  import Plug.Conn
  import Phoenix.Controller
  alias LogflareWeb.Router.Helpers, as: Routes

  def init(_params) do
  end

  def call(conn, _params) do
    if length(conn.assigns.user.sources) < 101 do
      conn
    else
      conn
      |> put_flash(:error, "You have 100 sources. Delete one first!")
      |> redirect(to: Routes.source_path(conn, :dashboard))
      |> halt()
    end
  end
end
