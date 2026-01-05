defmodule LogflareWeb.Plugs.AuthMustBeTeamAdmin do
  @moduledoc """
  Verifies the current user is an account owner OR an admin team member.

  WARNING: This plug does NOT verify ownership of specific resources.
  Controllers must separately verify that any resource IDs in params
  belong to the current user's team before performing destructive actions.
  """
  use Plug.Builder
  use LogflareWeb, :routes

  import Plug.Conn
  import Phoenix.Controller

  alias Logflare.TeamUsers.TeamUser
  alias Logflare.Teams.TeamContext
  @spec call(Plug.Conn.t(), any()) :: Plug.Conn.t()
  def call(%{assigns: assigns} = conn, _params) do
    team_context =
      assigns
      |> Map.get(:team_context)
      |> maybe_set_team_user(Map.get(assigns, :team_user))

    if admin_or_owner?(team_context) do
      conn
    else
      reject(conn, team_context)
    end
  end

  @spec reject(Plug.Conn.t(), TeamContext.t() | nil) :: Plug.Conn.t()
  defp reject(conn, %TeamContext{user: user}) when is_struct(user) do
    conn
    |> put_flash(
      :error,
      [
        "You're not the account owner or an admin. Please contact ",
        Phoenix.HTML.Link.link(user.name || user.email, to: "mailto:#{user.email}"),
        " for support."
      ]
    )
    |> redirect(to: ~p"/dashboard")
    |> halt()
  end

  defp reject(conn, _team_context) do
    conn
    |> put_flash(:error, "You're not the account owner or an admin.")
    |> redirect(to: ~p"/dashboard")
    |> halt()
  end

  defp admin_or_owner?(%TeamContext{} = team_context), do: TeamContext.team_admin?(team_context)

  defp admin_or_owner?(_team_context), do: false

  defp maybe_set_team_user(%TeamContext{} = team_context, %TeamUser{} = team_user),
    do: %TeamContext{team_context | team_user: team_user}

  defp maybe_set_team_user(team_context, _team_user), do: team_context
end
