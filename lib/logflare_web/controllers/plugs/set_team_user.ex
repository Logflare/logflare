defmodule LogflareWeb.Plugs.SetTeamUser do
  @moduledoc """
  Assigns team user if browser session is present in conn.
  """
  import Plug.Conn
  import Phoenix.Controller

  alias Logflare.Teams.TeamContext
  alias Logflare.TeamUsers
  use LogflareWeb, :routes

  def init(_), do: nil

  def call(conn, _opts) do
    case get_session(conn, :current_email) do
      nil ->
        assign(conn, :user, nil)

      email ->
        conn = set_team_user_for_browser(conn)
        user = Logflare.Users.Cache.get_by(email: email)
        teams = Logflare.Teams.list_teams_by_user_access(user)
        assign(conn, :teams, teams)
    end
  end

  def set_team_user_for_browser(conn) do
    current_email = get_session(conn, :current_email)

    conn =
      conn
      |> fetch_query_params()

    team_id = Map.get(conn.params, "team_id", nil)

    case TeamContext.resolve(team_id, current_email) do
      {:ok, %{user: user, team: team, team_user: team_user}} ->
        conn
        |> assign(:user, user)
        |> assign(:team, team)
        |> maybe_assign_team_user(team_user)

      {:error, :missing_team} ->
        user = Logflare.Users.Cache.get_by(email: current_email)

        {:ok, team} =
          Logflare.Teams.create_team(user, %{name: Logflare.Generators.team_name()})

        conn
        |> assign(:user, user)
        |> assign(:team, team)

      {:error, _} ->
        conn
    end
  end

  defp maybe_assign_team_user(conn, team_user) when is_struct(team_user),
    do: assign(conn, :team_user, team_user)

  defp maybe_assign_team_user(conn, _team_user), do: conn

  defp error(conn) do
    conn
    |> put_flash(
      :error,
      "Something went wrong. If this continues please contact support."
    )
    |> redirect(to: ~p"/dashboard")
  end

  defp drop(conn) do
    conn
    |> configure_session(drop: true)
    |> redirect(to: Routes.auth_path(conn, :login, team_user_deleted: true))
  end
end
