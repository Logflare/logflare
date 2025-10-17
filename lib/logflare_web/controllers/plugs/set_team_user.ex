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
        set_team_user_for_browser(conn, email)
    end
  end

  def set_team_user_for_browser(conn, email) do
    current_email = get_session(conn, :current_email)
    user = Logflare.Users.Cache.get(user_id)

    conn =
      conn
      |> fetch_query_params()

    team_id = Map.get(conn.params, "team_id", nil) |> dbg

    case TeamContext.resolve(user, team_id, current_email) do
      {:ok, %{user: user, team: team, team_user: team_user}} ->
        conn
        |> assign(:user, user)
        |> assign(:team, team)
        |> assign(:team_user, team_user)

      {:error, _} ->
        conn
    end
  end

  defp set_team_user(conn, team_user_id) do
    case TeamUsers.get_team_user(team_user_id) do
      nil ->
        drop(conn)

      t ->
        case TeamUsers.touch_team_user(t) do
          {1, [team_user]} ->
            team_user |> TeamUsers.preload_defaults()

            conn
            |> assign(:team_user, team_user)

          _ ->
            error(conn)
        end
    end
  end

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
