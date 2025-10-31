defmodule LogflareWeb.AuthLive do
  @moduledoc """
  Auth hooks for LiveViews.

  The selected team can be set with the `team_id` query param.
  It is verified by checking for a team user with the given email and team_id.
  """

  import Phoenix.Component

  alias Logflare.Teams.TeamContext

  def on_mount(:default, params, %{"current_email" => email}, socket) do
    team_id = Map.get(params, "team_id")

    case TeamContext.resolve(team_id, email) do
      {:ok, %TeamContext{team: team, user: user, team_user: team_user}} ->
        {:cont,
         assign(socket,
           user: Logflare.Users.preload_defaults(user),
           team: Logflare.Teams.preload_team_users(team),
           team_user: team_user
         )}

      {:error, _reason} ->
        {:halt, socket}
    end
  end

  def on_mount(:default, _params, _session, socket),
    do: {:halt, socket}
end
