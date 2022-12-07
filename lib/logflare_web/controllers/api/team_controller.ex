defmodule LogflareWeb.Api.TeamController do
  use LogflareWeb, :controller
  alias Logflare.Teams
  action_fallback LogflareWeb.Api.FallbackController

  def index(%{assigns: %{user: user}} = conn, _) do
    teams = Teams.list_teams_by_user_access(user)
    json(conn, teams)
  end

  def show(%{assigns: %{user: user}} = conn, %{"token" => token}) do
    with team when not is_nil(team) <- Teams.get_team_by_user_access(user, token),
         team <- Teams.preload_fields(team, [:user, :team_users]) do
      json(conn, team)
    end
  end

  def create(%{assigns: %{user: user}} = conn, params) do
    with {:ok, team} <- Teams.create_team(user, params),
         team <- Teams.preload_fields(team, [:user, :team_users]) do
      conn
      |> put_status(201)
      |> json(team)
    end
  end

  def update(%{assigns: %{user: user}} = conn, %{"token" => token} = params) do
    with team when not is_nil(team) <- Teams.get_team_by(token: token, user_id: user.id),
         {:ok, team} <- Teams.update_team(team, params),
         team <- Teams.preload_fields(team, [:user, :team_users]) do
      conn
      |> put_status(204)
      |> json(team)
    end
  end

  def delete(%{assigns: %{user: user}} = conn, %{"token" => token}) do
    with team when not is_nil(team) <- Teams.get_team_by(token: token, user_id: user.id),
         {:ok, _} <- Teams.delete_team(team) do
      conn
      |> Plug.Conn.send_resp(204, [])
      |> Plug.Conn.halt()
    end
  end
end
