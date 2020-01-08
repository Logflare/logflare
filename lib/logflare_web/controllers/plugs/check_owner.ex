defmodule LogflareWeb.Plugs.CheckOwner do
  @moduledoc """
  Checks for team_user in the assigns, rejects if exists.
  """
  use Plug.Builder

  import Plug.Conn
  import Phoenix.Controller

  alias LogflareWeb.Router.Helpers, as: Routes

  @doc """
  If I have a `team_user` in the assigns they're not the owner.
  """

  def call(%{assigns: %{team_user: team_user}} = conn, _params) do
    owner_email = conn.assigns.user.email
    owner_name = conn.assigns.user.name || conn.assigns.user.email

    conn
    |> put_flash(
      :error,
      [
        "You're not the account owner. Please contact ",
        Phoenix.HTML.Link.link(owner_name, to: "mailto:#{owner_email}"),
        " for support."
      ]
    )
    |> redirect(to: Routes.source_path(conn, :dashboard))
    |> halt()
  end

  def call(conn, _params) do
    conn
  end
end
