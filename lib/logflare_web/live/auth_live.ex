defmodule LogflareWeb.AuthLive do
  @moduledoc """
  Auth hooks for LiveViews.

  The selected team can be set with the `team_id` query param.
  It is verified by checking for a team user with the given email and team_id.
  """

  import Phoenix.Component

  use LogflareWeb, :routes

  alias Logflare.Teams.TeamContext

  def on_mount(:default, params, %{"current_email" => email}, socket) do
    team_id = Map.get(params, "t")

    case TeamContext.resolve(team_id, email) do
      {:ok, %TeamContext{team: team, user: user, team_user: team_user}} ->
        {:cont,
         assign(socket,
           user: Logflare.Users.preload_defaults(user),
           team: Logflare.Teams.preload_team_users(team),
           team_user: team_user
         )}

      {:error, _reason} ->
        # Shouldn't ever actually hit this branch. Invalid credential will have been caught in the Plug pipeline.
        {:halt,
         socket
         |> Phoenix.LiveView.redirect(to: ~p"/auth/login")}
    end
  end

  def on_mount(:default, _params, _session, socket),
    do: {:halt, socket}

  @doc """
  Assigns the context for a resource.
  Used for overriding the default context assignment when accessing  a resource.

  Resource must have a user_id field.
  Access should be verified first by the caller.

  Raises  if the team context cannot be succesfully assigned for a given resource and user email.
  """
  def assign_context_by_resource(socket, resource, user_email)
      when is_map_key(resource, :user_id) do
    resource = Logflare.Repo.preload(resource, user: :team)

    case TeamContext.resolve(resource.user.team.id, user_email) do
      {:ok, %TeamContext{team: team, user: user, team_user: team_user}} ->
        assign(socket,
          user: Logflare.Users.preload_defaults(user),
          team: Logflare.Teams.preload_team_users(team),
          team_user: team_user
        )

      {:error, _reason} ->
        socket
    end
  end
end
