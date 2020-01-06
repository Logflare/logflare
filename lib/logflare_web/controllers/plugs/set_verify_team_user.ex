defmodule LogflareWeb.Plugs.SetVerifyTeamUser do
  @moduledoc """
  Assigns team user if browser session is present in conn
  """
  import Plug.Conn
  import Phoenix.Controller
  alias Logflare.TeamUsers

  def init(_), do: nil

  def call(conn, opts), do: set_team_user_for_browser(conn, opts)

  def set_team_user_for_browser(conn, _opts) do
    case get_session(conn, :team_user_id) do
      nil ->
        conn

      team_user_id ->
        team_user = TeamUsers.get_team_user_and_preload(team_user_id)

        conn
        |> assign(:team_user, team_user)
    end
  end
end
