defmodule LogflareWeb.Plugs.SetTeam do
  @moduledoc """
  Assigns team user if browser session is present in conn
  """
  import Plug.Conn

  alias Logflare.Teams
  alias Logflare.User

  def init(_), do: nil

  def call(%{assigns: %{user: %User{}}} = conn, opts), do: set_team_for_browser(conn, opts)

  def call(conn, _opts), do: conn

  def set_team_for_browser(%{assigns: %{user: user}} = conn, _opts) do
    team =
      Teams.get_team_by(user_id: user.id)
      |> Teams.preload_team_users()

    conn
    |> assign(:team, team)
  end
end
