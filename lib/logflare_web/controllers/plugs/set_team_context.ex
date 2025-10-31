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

      _email ->
        set_team_user_for_browser(conn)
    end
  end

  def set_team_user_for_browser(conn) do
    current_email = get_session(conn, :current_email)
    team_id = Map.get(conn.params, "team_id", nil)

    case TeamContext.resolve(team_id, current_email) do
      {:ok, %{user: user, team: team, team_user: team_user}} ->
        teams = Logflare.Teams.list_teams_by_user_access(team_user || user)

        conn
        |> assign(:user, user)
        |> assign(:team, team)
        |> assign(:teams, teams)
        |> maybe_assign_team_user(team_user)

      {:error, :team_not_found} ->
        drop(conn)

      {:error, _} ->
        error(conn)
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
