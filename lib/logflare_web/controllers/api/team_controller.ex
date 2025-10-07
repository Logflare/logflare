defmodule LogflareWeb.Api.TeamController do
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  alias Logflare.Teams
  alias Logflare.TeamUsers
  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias LogflareWeb.OpenApi.Accepted
  alias LogflareWeb.OpenApi.Created
  alias LogflareWeb.OpenApi.List
  alias LogflareWeb.OpenApi.NotFound
  alias LogflareWeb.OpenApi.UnprocessableEntity

  alias LogflareWeb.OpenApiSchemas.Team

  action_fallback(LogflareWeb.Api.FallbackController)

  tags(["management"])

  operation(:index,
    summary: "List teams",
    responses: %{200 => List.response(Team)}
  )

  def index(%{assigns: %{user: user}} = conn, _) do
    teams = Teams.list_teams_by_user_access(user)
    json(conn, teams)
  end

  operation(:show,
    summary: "Fetch team",
    parameters: [token: [in: :path, description: "Team Token", type: :string]],
    responses: %{
      200 => Team.response(),
      404 => NotFound.response()
    }
  )

  def show(%{assigns: %{user: user}} = conn, %{"token" => token}) do
    with team when not is_nil(team) <- Teams.get_team_by_user_access(user, token),
         team <- Teams.preload_fields(team, [:user, :team_users]) do
      json(conn, team)
    end
  end

  operation(:create,
    summary: "Create Team",
    request_body: Team.params(),
    responses: %{
      201 => Created.response(Team),
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
    request_body: Team.params(),
    responses: %{
      201 => Created.response(Team),
      204 => Accepted.response(),
      404 => NotFound.response(),
      422 => UnprocessableEntity.response()
    }
  )

  def update(%{assigns: %{user: user}} = conn, %{"token" => token} = params) do
    with team when not is_nil(team) <- Teams.get_team_by(token: token, user_id: user.id),
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
    with team when not is_nil(team) <- Teams.get_team_by(token: token, user_id: user.id),
         {:ok, _} <- Teams.delete_team(team) do
      conn
      |> Plug.Conn.send_resp(204, [])
      |> Plug.Conn.halt()
    end
  end

  operation(:add_member,
    summary: "Add Team Member",
    parameters: [
      token: [in: :path, description: "Team Token", type: :string],
      id: [in: :path, description: "User ID as an email", type: :string]
    ],
    responses: %{
      204 => Accepted.response(),
      404 => NotFound.response()
    }
  )

  def add_member(%{assigns: %{user: user}} = conn, %{"token" => token, "id" => id}) do
    auth_params = %{
      email: id
    }

    with team when not is_nil(team) <- Teams.get_team_by(token: token, user_id: user.id),
         {:ok, _} <- TeamUsers.insert_or_update_team_user(team, auth_params) do
      BigQueryAdaptor.update_iam_policy()
      BigQueryAdaptor.patch_dataset_access(team.user)

      conn
      |> Plug.Conn.send_resp(204, [])
      |> Plug.Conn.halt()
    end
  end

  operation(:delete_member,
    summary: "Delete Team Member",
    parameters: [
      token: [in: :path, description: "Team Token", type: :string],
      id: [in: :path, description: "User ID as an email", type: :string]
    ],
    responses: %{
      204 => Accepted.response(),
      404 => NotFound.response()
    }
  )

  def delete_member(%{assigns: %{user: user}} = conn, %{"token" => token, "id" => id}) do
    auth_params = %{
      email: id
    }

    team_user = TeamUsers.get_team_user!(auth_params)

    with team when not is_nil(team) <- Teams.get_team_by(token: token, user_id: user.id),
         {:ok, _} <- TeamUsers.delete_team_user(team_user) do
      BigQueryAdaptor.update_iam_policy()
      BigQueryAdaptor.patch_dataset_access(team.user)

      conn
      |> Plug.Conn.send_resp(204, [])
      |> Plug.Conn.halt()
    end
  end
end
