defmodule LogflareWeb.Plugs.CheckAdmin do
  @moduledoc """
  Verifies that user is admin
  """
  use Plug.Builder

  import Plug.Conn
  import Phoenix.Controller
  alias Logflare.Admin

  alias LogflareWeb.ErrorView

  def call(conn, _params) do
    email = Plug.Conn.get_session(conn, :current_email)

    if Admin.admin?(email) do
      conn
      |> assign(:admin, true)
    else
      conn
      |> put_status(403)
      |> put_view(ErrorView)
      |> render("403_page.html", conn.assigns)
      |> halt()
    end
  end
end
