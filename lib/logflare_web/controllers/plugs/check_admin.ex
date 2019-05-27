defmodule LogflareWeb.Plugs.CheckAdmin do
  use Plug.Builder

  import Plug.Conn
  import Phoenix.Controller

  alias LogflareWeb.Router.Helpers, as: Routes

  def call(%{assigns: %{user: %{admin: true}}} = c, _params), do: c

  def call(conn, _params) do
    conn
    |> put_status(401)
    |> put_flash(:error, "You're not an admin!")
    |> redirect(to: Routes.source_path(conn, :dashboard))
    |> halt()
  end
end
