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

  def init(_), do: nil

  def call(%{assigns: %{user: %User{team: team}}} = conn, opts) when is_nil(team),
    do: set_team(conn, opts)

  def call(conn, opts), do: conn

  def set_team(%{assigns: %{user: user}} = conn, opts) do
    changeset = User.changeset(user, %{team: Generators.team_name()})

    case Repo.update(changeset) do
      {:ok, user} ->
        assign(conn, :user, user)

      {:error, changeset} ->
        Logger.warn("Team was nil and updating user failed for user: #{user.email}")
        conn
    end
  end
end
