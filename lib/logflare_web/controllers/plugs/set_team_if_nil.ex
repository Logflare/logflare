defmodule LogflareWeb.Plugs.SetTeamIfNil do
  @moduledoc """
  Assigns team user if browser session is present in conn
  """
  require Logger

  import Plug.Conn
  import Phoenix.Controller

  alias Logflare.User
  alias Logflare.Generators
  alias Logflare.Repo
  alias Logflare.Teams
  alias Logflare.Teams.Team

  def init(_), do: nil

  def call(%{assigns: %{user: %User{team: team}}} = conn, opts) when is_nil(team),
    do: set_team(conn, opts)

  def call(conn, opts), do: conn

  def set_team(%{assigns: %{user: user}} = conn, opts) do
    {:ok, team} = Teams.create_team(user, %{name: Generators.team_name()})

    conn
    |> assign(:user, user)
    |> assign(:team, team)
  end
end
