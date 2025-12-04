defmodule LogflareWeb.Plugs.AuthMustBeOwner do
  @moduledoc """
  Verifies the current user is an account owner (not a team member).

  WARNING: This plug does NOT verify ownership of specific resources.
  Controllers must separately verify that any resource IDs in params
  belong to the current user's team before performing destructive actions.
  """
  use Plug.Builder

  import Plug.Conn
  import Phoenix.Controller

  use LogflareWeb, :routes

  def call(%{assigns: %{user: user, team_user: _team_user}} = conn, _params),
    do: reject(conn, user)

  def call(%{assigns: %{user: _user}} = conn, _params), do: conn

  def reject(conn, user) do
    conn
    |> put_flash(
      :error,
      [
        "You're not the account owner. Please contact ",
        Phoenix.HTML.Link.link(user.name || user.email, to: "mailto:#{user.email}"),
        " for support."
      ]
    )
    |> redirect(to: ~p"/dashboard")
    |> halt()
  end
end
