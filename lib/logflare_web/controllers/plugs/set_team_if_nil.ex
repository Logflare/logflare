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

  def call(conn, _opts) do
    with current_email when is_binary(current_email) <- get_session(conn, :current_email),
         user = %User{team: nil} <- Logflare.Users.get_by_and_preload(email: current_email) do
      {:ok, _team} = Teams.create_team(user, %{name: Logflare.Generators.team_name()})

      now =
        DateTime.utc_now()
        |> DateTime.truncate(:second)
        |> DateTime.to_naive()

      user
      |> Ecto.Changeset.change(%{updated_at: now})
      |> Logflare.Repo.update!()

      conn
    else
      _ -> conn
    end
  end
end
