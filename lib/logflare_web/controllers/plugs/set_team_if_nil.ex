defmodule LogflareWeb.Plugs.SetTeamIfNil do
  @moduledoc """
  Assigns team user if browser session is present in conn
  """
  require Logger
  use Logflare.Commons

  import Plug.Conn

  alias Logflare.User
  alias Logflare.Generators
  alias Logflare.Teams

  def init(_), do: nil

  def call(%{assigns: %{user: %User{team: team}}} = conn, opts) when is_nil(team),
    do: set_team(conn, opts)

  def call(conn, _opts), do: conn

  def set_team(%{assigns: %{user: %User{team: nil} = user}} = conn, _opts) do
    {:ok, _team} = Teams.create_team(user, %{name: Generators.team_name()})
    user = Users.get_user(user.id)

    assign(conn, :user, user)
  end
end
