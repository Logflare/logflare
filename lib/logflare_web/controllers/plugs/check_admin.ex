defmodule LogflareWeb.Plugs.CheckAdmin do
  import Plug.Conn
  import Phoenix.Controller

  alias LogflareWeb.Router.Helpers, as: Routes

  def init(_params) do
  end

  def call(conn, _params) do
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
