defmodule LogflareWeb.Api.TeamController do
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  alias Logflare.Teams
  alias Logflare.Teams.Team
  alias Logflare.TeamUsers
  alias Logflare.TeamUsers.TeamUser
  alias Logflare.User
  alias Logflare.Users
  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias LogflareWeb.OpenApi.Accepted
  alias LogflareWeb.OpenApi.Created
  alias LogflareWeb.OpenApi.List
  alias LogflareWeb.OpenApi.NotFound
  alias LogflareWeb.OpenApi.UnprocessableEntity

  require Logger

  alias LogflareWeb.OpenApiSchemas.Team, as: TeamSchema
  alias LogflareWeb.OpenApiSchemas.TeamUser, as: TeamUserSchema

  action_fallback(LogflareWeb.Api.FallbackController)

  tags(["management"])

  operation(:index,
    summary: "List teams",
    responses: %{200 => List.response(TeamSchema)}
  )

  def index(%{assigns: %{user: user}} = conn, _) do
    teams = Teams.list_teams_by_user_access(user)
    json(conn, teams)
  end

  operation(:show,
    summary: "Fetch team",
    parameters: [token: [in: :path, description: "Team Token", type: :string]],
    responses: %{
      200 => TeamSchema.response(),
      404 => NotFound.response()
    }
  )

  def show(%{assigns: %{user: user}} = conn, %{"token" => token}) do
    with %Team{} = team <- Teams.get_team_by_user_access(user, token),
         team <- Teams.preload_fields(team, [:user, :team_users]) do
      json(conn, team)
    end
  end

  operation(:create,
    summary: "Create Team",
    request_body: TeamSchema.params(),
    responses: %{
      201 => Created.response(TeamSchema),
      404 => NotFound.response(),
      422 => UnprocessableEntity.response()
    }
  )

  def create(%{assigns: %{user: user}} = conn, params) do
    with {:ok, team} <- Teams.create_team(user, params),
         team <- Teams.preload_fields(team, [:user, :team_users]) do
      conn
      |> put_status(201)
      |> json(team)
    end
  end

  operation(:update,
    summary: "Update team",
    parameters: [token: [in: :path, description: "Team Token", type: :string]],
    request_body: TeamSchema.params(),
    responses: %{
      201 => Created.response(TeamSchema),
      204 => Accepted.response(),
      404 => NotFound.response(),
      422 => UnprocessableEntity.response()
    }
  )

  def update(%{assigns: %{user: user}} = conn, %{"token" => token} = params) do
    with %Team{} = team <- Teams.get_team_by(token: token, user_id: user.id),
         {:ok, team} <- Teams.update_team(team, params),
         team <- Teams.preload_fields(team, [:user, :team_users]) do
      conn
      |> case do
        %{method: "PUT"} ->
          conn
          |> put_status(201)
          |> json(team)

        %{method: "PATCH"} ->
          conn |> send_resp(204, "")
      end
    end
  end

  operation(:delete,
    summary: "Delete Team",
    parameters: [token: [in: :path, description: "Team Token", type: :string]],
    responses: %{
      204 => Accepted.response(),
      404 => NotFound.response()
    }
  )

  def delete(%{assigns: %{user: user}} = conn, %{"token" => token}) do
    with %Team{} = team <- Teams.get_team_by(token: token, user_id: user.id),
         {:ok, _} <- Teams.delete_team(team) do
      conn
      |> Plug.Conn.send_resp(204, [])
      |> Plug.Conn.halt()
    end
  end

  operation(:add_member,
    summary: "Add Team Member",
    parameters: [
      token: [in: :path, description: "Team Token", type: :string]
    ],
    request_body: TeamUserSchema.params(),
    responses: %{
      204 => Accepted.response(),
      404 => NotFound.response()
    }
  )

  def add_member(%{assigns: %{user: user}} = conn, %{"team_token" => token, "email" => email}) do
    auth_params =
      case Users.get_by(email: email) do
        nil ->
          {:ok, new_user} = Users.insert_user(%{email: email, provider: user.provider})
          Logger.info("Created new user #{email}")

          %{
            email: new_user.email,
            provider_uid: new_user.provider_uid,
            provider: new_user.provider
          }

        %User{} = existing_user ->
          %{
            email: existing_user.email,
            provider_uid: existing_user.provider_uid,
            provider: existing_user.provider
          }
      end

    with %Team{} = team <- Teams.get_team_by(token: token, user_id: user.id),
         team <- Teams.preload_user(team),
         {:ok, _} <- TeamUsers.insert_or_update_team_user(team, auth_params) do
      BigQueryAdaptor.update_iam_policy()
      BigQueryAdaptor.patch_dataset_access(team.user)

      conn
      |> Plug.Conn.send_resp(204, [])
      |> Plug.Conn.halt()
    end
  end

  operation(:remove_member,
    summary: "Remove Team Member",
    parameters: [
      token: [in: :path, description: "Team Token", type: :string],
      id: [in: :path, description: "User ID as an email", type: :string]
    ],
    responses: %{
      204 => Accepted.response(),
      404 => NotFound.response()
    }
  )

  def remove_member(%{assigns: %{user: user}} = conn, %{"team_token" => token, "id" => id}) do
    with %TeamUser{} = team_user <- TeamUsers.get_team_user_by(email: id),
         %Team{} = team <- Teams.get_team_by(token: token, user_id: user.id),
         team <- Teams.preload_user(team),
         {:ok, _} <- TeamUsers.delete_team_user(team_user) do
      BigQueryAdaptor.update_iam_policy()
      BigQueryAdaptor.patch_dataset_access(team.user)

      conn
      |> Plug.Conn.send_resp(204, [])
      |> Plug.Conn.halt()
    end
  end
end
