defmodule LogflareWeb.AuthLive do
  @moduledoc """
  Auth hooks for LiveViews.

  The selected team can be set with the `team_id` query param.
  It is verified by checking for a team user with the given email and team_id.
  """

  import Phoenix.Component
  import Phoenix.LiveView, only: [attach_hook: 4, push_navigate: 2]

  use LogflareWeb, :routes

  alias Logflare.Repo
  alias Logflare.Teams.Team
  alias Logflare.Teams.TeamContext
  alias LogflareWeb.Utils

  require Logger

  def on_mount(:default, params, %{"current_email" => email} = session, socket) do
    team_id = Map.get(params, "t") || session["last_switched_team_id"]

    case TeamContext.resolve(team_id, email) do
      {:ok, %TeamContext{team: team, user: user, team_user: team_user} = team_context} ->
        {:cont,
         assign(socket,
           user: Logflare.Users.preload_defaults(user),
           team: Logflare.Teams.preload_team_users(team),
           team_user: team_user,
           team_context: team_context
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
      |> maybe_assign_team_from_resource(socket)
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
      {:ok, %TeamContext{team: team, user: user, team_user: team_user} = team_context} ->
        assign(socket,
          user: Logflare.Users.preload_defaults(user),
          team: Logflare.Teams.preload_team_users(team),
          team_user: team_user,
          team_context: team_context
        )

      {:error, reason} ->
        raise "Unable to resolve team context for team_id #{team_id}: #{inspect(reason)}"
    end
  end

  defp ensure_team_param(params, uri, socket) do
    socket = maybe_assign_team_from_resource(params, socket)
    team = socket.assigns[:team]

    cond do
      team_param_matches?(params, team) ->
        {:cont, socket}

      is_struct(team, Team) ->
        path = uri |> uri_to_local_path() |> Utils.with_team_param(team)
        {:halt, push_navigate(socket, to: path, replace: true)}

      true ->
        {:cont, socket}
    end
  end

  defp team_param_matches?(%{"t" => team_id}, %Team{id: id}), do: team_id == to_string(id)
  defp team_param_matches?(_params, _team), do: false

  defp maybe_assign_team_from_resource(
         %{"t" => _team_id},
         %{view: LogflareWeb.QueryLive} = socket
       ),
       do: socket

  defp maybe_assign_team_from_resource(params, %{view: view} = socket) do
    effective_user = socket.assigns[:team_user] || socket.assigns.user

    case TeamContext.resource_team_id_query(view, params, effective_user) do
      nil ->
        socket

      query ->
        case Repo.all(query) do
          [team_id] -> assign_context_by_team_id(socket, team_id, effective_user.email)
          _ -> socket
        end
    end
  end

  defp uri_to_local_path(uri) do
    %URI{path: path, query: query} = URI.parse(uri)
    %URI{path: path, query: query} |> to_string()
  end
end
