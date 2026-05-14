defmodule LogflareWeb.AuthLive do
  @moduledoc """
  Auth hooks for LiveViews.

  The selected team can be set with the `team_id` query param.
  It is verified by checking for a team user with the given email and team_id.
  """

  import Phoenix.Component
  import Phoenix.LiveView, only: [attach_hook: 4, push_patch: 2]

  use LogflareWeb, :routes

  alias Logflare.Repo
  alias Logflare.Teams.Team
  alias Logflare.Teams.TeamContext
  alias LogflareWeb.Utils

  require Logger

  def on_mount(:default, params, %{"current_email" => email} = session, socket) do
    team_id = Map.get(params, "t") || session["last_switched_team_id"]

    case TeamContext.resolve(team_id, email) do
      {:ok, %TeamContext{team: team, user: user, team_user: team_user}} ->
        {:cont,
         assign(socket,
           user: Logflare.Users.preload_defaults(user),
           team: Logflare.Teams.preload_team_users(team),
           team_user: team_user
         )}

      {:error, _reason} = error ->
        Logger.warning(
          "Error resolving team context for email #{email}, team_id #{team_id}: #{inspect(error)}"
        )

        # Shouldn't ever actually hit this branch. Invalid credential will have been caught in the Plug pipeline.
        {:halt,
         socket
         |> Phoenix.LiveView.redirect(to: ~p"/auth/login")}
    end
  end

  def on_mount(:default, _params, session, socket) do
    Logger.warning(
      "No current_email in session during LiveView mount, session keys: #{inspect(Map.keys(session))}"
    )

    {:halt, Phoenix.LiveView.redirect(socket, to: ~p"/auth/login")}
  end

  def on_mount(:ensure_team_param, params, _session, socket) do
    socket =
      params
      |> maybe_assign_team_from_resource("", socket)
      |> attach_hook(:ensure_team_param, :handle_params, &ensure_team_param/3)

    {:cont, socket}
  end

  @doc """
  Assigns the context for a resource.
  Used for overriding the default context assignment when accessing  a resource.

  Resource must have a user_id field.
  Access should be verified first by the caller.

  Raises  if the team context cannot be succesfully assigned for a given resource and user email.
  """
  def assign_context_by_resource(socket, resource, user_email)
      when is_map_key(resource, :user_id) do
    resource = Repo.preload(resource, user: :team)
    assign_context_by_team_id(socket, resource.user.team.id, user_email)
  end

  def assign_context_by_resource(socket, nil, _user_email), do: socket

  @spec assign_context_by_team_id(
          Phoenix.LiveView.Socket.t(),
          TeamContext.team_id_param(),
          String.t()
        ) :: Phoenix.LiveView.Socket.t()
  def assign_context_by_team_id(socket, nil, _user_email), do: socket

  def assign_context_by_team_id(socket, team_id, user_email) do
    case TeamContext.resolve(team_id, user_email) do
      {:ok, %TeamContext{team: team, user: user, team_user: team_user}} ->
        assign(socket,
          user: Logflare.Users.preload_defaults(user),
          team: Logflare.Teams.preload_team_users(team),
          team_user: team_user
        )

      {:error, reason} ->
        raise "Unable to resolve team context for team_id #{team_id}: #{inspect(reason)}"
    end
  end

  defp ensure_team_param(params, uri, socket) do
    current_uri = local_path_with_query(uri)
    socket = maybe_assign_team_from_resource(params, uri, socket)
    team = socket.assigns[:team]

    cond do
      team_param_matches?(params, team) ->
        {:cont, socket}

      is_struct(team, Team) ->
        next_uri = Utils.with_team_param(current_uri, team)
        {:halt, push_patch(socket, to: next_uri, replace: true)}

      true ->
        {:cont, socket}
    end
  end

  defp team_param_matches?(%{"t" => team_id}, %Team{id: id}), do: team_id == to_string(id)
  defp team_param_matches?(_params, _team), do: false

  defp maybe_assign_team_from_resource(params, uri, %{view: view} = socket) do
    if function_exported?(view, :resource_team_id_query, 3) do
      effective_user = socket.assigns[:team_user] || socket.assigns.user

      case view.resource_team_id_query(params, uri, effective_user) do
        nil -> socket
        query -> assign_context_by_team_id(socket, Repo.one(query), effective_user.email)
      end
    else
      socket
    end
  end

  defp local_path_with_query(uri) do
    uri = URI.parse(uri)

    if uri.query in [nil, ""] do
      uri.path
    else
      uri.path <> "?" <> uri.query
    end
  end
end
