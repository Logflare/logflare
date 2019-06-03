defmodule LogflareWeb.Plugs.CheckAdmin do
  @moduledoc """
  Verifies that user is admin
  """
  use Plug.Builder

  import Plug.Conn
  import Phoenix.Controller

  alias LogflareWeb.Router.Helpers, as: Routes

  def call(%{assigns: %{user: %{admin: true}}} = c, _params), do: c

  def call(conn, _params) do
    conn
    |> put_status(403)
    |> put_flash(:error, "You're not an admin!")
    |> redirect(to: Routes.source_path(conn, :dashboard))
    |> halt()
  end
end
