defmodule LogflareWeb.Plugs.SetTeamUser do
  @moduledoc """
  Assigns team user if browser session is present in conn.
  """
  import Plug.Conn
  import Phoenix.Controller

  alias Logflare.TeamUsers
  alias LogflareWeb.Router.Helpers, as: Routes

  def init(_), do: nil

  def call(conn, opts), do: set_team_user_for_browser(conn, opts)

  def set_team_user_for_browser(conn, _opts) do
    case get_session(conn, :team_user_id) do
      nil ->
        conn

      team_user_id ->
        set_team_user(conn, team_user_id)
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
    |> redirect(to: Routes.source_path(conn, :dashboard))
  end

  defp drop(conn) do
    conn
    |> configure_session(drop: true)
    |> redirect(to: Routes.auth_path(conn, :login, team_user_deleted: true))
  end
end
