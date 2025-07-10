defmodule LogflareWeb.Plugs.SetTeamIfNil do
  @moduledoc """
  Creates and assigns a team for the user if they do not have one yet.
  """

  @behaviour Plug

  import Plug.Conn

  alias Logflare.User
  alias Logflare.Teams

  require Logger

  def init(_), do: []

  def call(%{assigns: %{user: %User{team: nil} = user}} = conn, _opts) do
    {:ok, team} = Teams.create_team(user, %{name: Logflare.Generators.team_name()})

    conn
    |> assign(:user, user)
    |> assign(:team, team)
  end

  def call(conn, _opts), do: conn
end
