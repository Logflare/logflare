defmodule LogflareWeb.Plugs.AuthSwitch do
  @moduledoc """
  Assigns team user if browser session is present in conn
  """
  require Logger

  import Plug.Conn
  import Phoenix.Controller

  alias Logflare.Users
  alias Logflare.TeamUsers
  alias LogflareWeb.Router.Helpers, as: Routes

  def init(_), do: nil

  @doc """
    As a team user I should not be able to switch to another team user who is not me.
  """

  def call(
        %{
          assigns: %{user: user, team_user: team_user, team: team},
          query_params: %{"user_id" => user_id, "team_user_id" => team_user_id}
        } = conn,
        opts
      ) do
    switch_to_team_user = TeamUsers.get_team_user!(team_user_id)

    if is_nil(switch_to_team_user) do
      reject(conn, opts)
    else
      verify(team_user.provider_uid, switch_to_team_user.provider_uid, conn, opts)
    end
  end

  @doc """
    As a team user I should not be able to switch to an account owner who is not me.
  """

  def call(
        %{
          assigns: %{user: user, team_user: team_user, team: team},
          query_params: %{"user_id" => user_id}
        } = conn,
        opts
      ) do
    switch_to_user = Users.get(user_id)

    if is_nil(switch_to_user) do
      reject(conn, opts)
    else
      verify(team_user.provider_uid, switch_to_user.provider_uid, conn, opts)
    end
  end

  @doc """
    As an account owner I should not be able to switch to another team as a team user who is not me.
  """

  def call(
        %{
          assigns: %{user: user, team: team},
          query_params: %{"user_id" => user_id, "team_user_id" => team_user_id}
        } = conn,
        opts
      ) do
    team_user = TeamUsers.get_team_user!(team_user_id)

    if is_nil(team_user) do
      reject(conn, opts)
    else
      verify(user.provider_uid, team_user.provider_uid, conn, opts)
    end
  end

  @doc """
   As an account owner I should not be able to switch to another account as a different account owner.
  """

  def call(
        %{assigns: %{user: user, team: team}, query_params: %{"user_id" => user_id}} = conn,
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
    |> redirect(to: Routes.source_path(conn, :dashboard))
    |> halt()
  end
end
