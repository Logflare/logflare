defmodule LogflareWeb.Plugs.CheckAdmin do
  use Plug.Builder

  import Plug.Conn
  import Phoenix.Controller

  alias LogflareWeb.Router.Helpers, as: Routes

  plug :verify_admin

  def verify_admin(conn, _params) do
    cond do
      is_nil(conn.assigns.user) ->
        conn
        |> put_flash(:error, "You're not an admin!")
        |> redirect(to: Routes.source_path(conn, :dashboard))
        |> halt()

      true ->
        if conn.assigns.user.admin == true do
          conn
        else
          conn
          |> put_flash(:error, "You're not an admin!")
          |> redirect(to: Routes.source_path(conn, :dashboard))
          |> halt()
        end
    end
  end
end
