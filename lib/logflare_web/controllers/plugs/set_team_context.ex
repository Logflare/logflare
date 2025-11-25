defmodule LogflareWeb.Plugs.SetTeamContext do
  @moduledoc """
  Assigns user and team if browser session is present in conn.
  """
  import Plug.Conn
  import Phoenix.Controller

  alias Logflare.Teams.TeamContext
  use LogflareWeb, :routes

  def init(_), do: nil

  def call(conn, _opts) do
    case get_session(conn, :current_email) do
      nil ->
        assign(conn, :user, nil)

      email ->
        team_id = Map.get(conn.params, "t", nil)
        set_team_context(conn, team_id, email)
    end
  end

  def set_team_context(conn, team_id, email) do
    case TeamContext.resolve(team_id, email) do
      {:ok, %{user: user, team: team, team_user: team_user}} ->
        teams = Logflare.Teams.list_teams_by_user_access(team_user || user)

        conn
        |> assign(:user, user)
        |> assign(:team, team)
        |> assign(:teams, teams)
        |> maybe_assign_team_user(team_user)

      {:error, :team_not_found} ->
        forbidden(conn, email)

      {:error, :invalid_team_id} ->
        drop_team_param(conn)

      {:error, :not_authorized} ->
        forbidden(conn, email)
    end
  end

  defp maybe_assign_team_user(conn, team_user) when is_struct(team_user),
    do: assign(conn, :team_user, team_user)

  defp maybe_assign_team_user(conn, _team_user), do: conn

  defp forbidden(conn, email) do
    user = Logflare.Users.get_by_and_preload(email: email)
    # if signed in as a team_user then user may be nil
    team = if user, do: user.team, else: nil

    conn =
      conn
      |> put_status(403)
      |> put_layout(false)
      |> put_view(LogflareWeb.ErrorView)
      |> assign(:user, user)
      |> assign(:team, team)
      |> LogflareWeb.Plugs.SetPlan.call([])
      |> halt()

    conn
    |> render("403_page.html", conn.assigns)
    |> halt()
  end

  defp drop_team_param(conn) do
    params =
      conn.query_params
      |> Map.drop(["t"])

    path =
      if params == %{} do
        conn.request_path
      else
        conn.request_path <> "?" <> Plug.Conn.Query.encode(params)
      end

    conn
    |> redirect(to: path)
    |> halt
  end
end
