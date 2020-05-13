defmodule LogflareWeb.Plugs.SetTeamUser do
  @moduledoc """
  Assigns team user if browser session is present in conn
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
        t = TeamUsers.get_team_user!(team_user_id)

        case TeamUsers.touch_team_user(t) do
          {1, [team_user]} ->
            team_user |> TeamUsers.preload_defaults()

            conn
            |> assign(:team_user, team_user)

          _ ->
            conn
            |> put_flash(
              :error,
              "Something went wrong. If this continues please contact support."
            )
            |> redirect(to: Routes.source_path(conn, :dashboard))
            |> halt()
        end
    end
  end
end
