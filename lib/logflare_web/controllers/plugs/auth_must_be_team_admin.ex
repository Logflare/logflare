defmodule LogflareWeb.Plugs.AuthMustBeTeamAdmin do
  @moduledoc """
  Verifies the current user is an account owner or team member with `admin` role.

  WARNING: This plug does NOT verify ownership of specific resources.
  Controllers must separately verify that any resource IDs in params
  belong to the current user's team before performing destructive actions.
  """
  use Plug.Builder
  use LogflareWeb, :routes

  import Plug.Conn
  import Phoenix.Controller

  alias Logflare.Teams.TeamContext

  @spec call(Plug.Conn.t(), any()) :: Plug.Conn.t()
  def call(%{assigns: %{team_context: %TeamContext{} = team_context}} = conn, _params) do
    if TeamContext.team_admin?(team_context) do
      conn
    else
      reject(conn, team_context)
    end
  end

  @spec reject(Plug.Conn.t(), TeamContext.t()) :: Plug.Conn.t()
  defp reject(conn, %TeamContext{user: user}) when is_struct(user) do
    conn
    |> put_flash(
      :error,
      [
        "You're not the account owner or an admin. Please contact ",
        PhoenixHTMLHelpers.Link.link(user.name || user.email, to: "mailto:#{user.email}"),
        " for support."
      ]
    )
    |> redirect(to: ~p"/dashboard")
    |> halt()
  end
end
