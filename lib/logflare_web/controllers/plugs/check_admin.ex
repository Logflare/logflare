defmodule LogflareWeb.Plugs.CheckAdmin do
  @moduledoc """
  Verifies that user is admin
  """
  use Plug.Builder

  import Plug.Conn
  import Phoenix.Controller

  alias LogflareWeb.ErrorView

  def call(%{assigns: %{user: %{admin: true}}} = c, _params), do: c

  def call(conn, _params) do
    conn
    |> put_status(403)
    |> put_view(ErrorView)
    |> render("403_page.html", conn.assigns)
    |> halt()
  end
end
