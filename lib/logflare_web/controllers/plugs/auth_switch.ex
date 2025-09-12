defmodule LogflareWeb.Plugs.AuthSwitch do
  @moduledoc """
  Assigns team user if browser session is present in conn
  """
  require Logger

  import Plug.Conn
  import Phoenix.Controller

  alias Logflare.Users
  alias Logflare.TeamUsers
  use LogflareWeb, :routes

  def init(_), do: nil

  @doc """
    As a team user I should not be able to switch to another team user who is not me.
  """

  def call(
        %{
          assigns: %{user: _user, team_user: team_user, team: _team},
          query_params: %{"user_id" => _user_id, "team_user_id" => team_user_id}
        } = conn,
        opts
      ) do
    case TeamUsers.get_team_user(team_user_id) do
      nil -> reject(conn, opts)
      switch_to_team_user -> verify(team_user.email, switch_to_team_user.email, conn, opts)
    end
  end

  # As a team user I should not be able to switch to an account owner who is not me.
  def call(
        %{
          assigns: %{user: _user, team_user: team_user, team: _team},
          query_params: %{"user_id" => user_id}
        } = conn,
        opts
      ) do
    case Users.get(user_id) do
      nil -> reject(conn, opts)
      switch_to_user -> verify(team_user.email, switch_to_user.email, conn, opts)
    end
  end

  # As an account owner I should not be able to switch to another team as a team user who is not me.
  def call(
        %{
          assigns: %{user: user, team: _team},
          query_params: %{"user_id" => _user_id, "team_user_id" => team_user_id}
        } = conn,
        opts
      ) do
    case TeamUsers.get_team_user(team_user_id) do
      nil -> reject(conn, opts)
      team_user -> verify(user.email, team_user.email, conn, opts)
    end
  end

  # As an account owner I should not be able to switch to another account as a different account owner.
  def call(
        %{assigns: %{user: user, team: _team}, query_params: %{"user_id" => user_id}} = conn,
        opts
      ) do
    verify(user_id, user.id, conn, opts)
  end

  # Don't fail down to a generic conn match so we don't accidentally auth someone
  # def call(conn, _opts), do: conn

  defp verify(from_id, to_id, conn, opts) do
    if from_id == to_id do
      conn
    else
      reject(conn, opts)
    end
  end

  defp reject(conn, _opts) do
    conn
    |> put_flash(:error, "You can't do that!")
    |> redirect(to: ~p"/dashboard")
    |> halt()
  end
end
