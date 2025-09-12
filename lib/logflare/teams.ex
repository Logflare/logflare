defmodule Logflare.Teams do
  @moduledoc false

  import Ecto.Query, warn: false

  alias Logflare.Repo
  alias Logflare.Teams.Team
  alias Logflare.TeamUsers.TeamUser
  alias Logflare.Users
  alias Logflare.User

  @doc "Returns a list of teams. Unfiltered."
  @spec list_teams() :: [Team.t()]
  def list_teams, do: Repo.all(Team)

  @doc "Gets a single team. Raises `Ecto.NoResultsError` if the Team does not exist."
  @spec get_team!(String.t() | number()) :: Team.t()
  def get_team!(id), do: Repo.get!(Team, id)

  @doc "Gets a single team by attribute. Returns nil if not found"
  @spec get_team_by(keyword()) :: Team.t() | nil
  def get_team_by(keyword), do: Repo.get_by(Team, keyword)

  @doc "Gets a user's home team. A home team is set on the `User`'s `:team` key. Uses email as an identifier."
  @spec get_home_team(TeamUser.t()) :: Team.t() | nil
  def get_home_team(%TeamUser{email: email}) do
    case Users.get_by(email: email) |> Users.preload_team() do
      nil -> nil
      %{team: team} -> team
    end
  end

  @doc "Preloads the `:user` assoc"
  @spec preload_user(nil | Team.t()) :: Team.t() | nil
  def preload_user(team), do: Repo.preload(team, :user)

  @doc "Preloads the `:team_users` assoc"
  @spec preload_team_users(nil | Team.t(), Keyword.t()) :: Team.t() | nil
  def preload_team_users(team, opts \\ []), do: Repo.preload(team, :team_users, opts)

  @doc "Preloads given fields of a Team"
  @spec preload_fields(nil | Team.t(), list(keyword() | atom())) :: Team.t() | nil
  def preload_fields(team, fields), do: Repo.preload(team, fields)

  @doc "Creates a team from a user."
  @spec create_team(User.t(), map()) :: {:ok, Team.t()} | {:error, Ecto.Changeset.t()}
  def create_team(user, attrs) do
    user
    |> Ecto.build_assoc(:team)
    |> Team.changeset(attrs)
    |> Repo.insert()
  end

  @doc "Updates a team"
  @spec update_team(Team.t(), map()) :: {:ok, Team.t()} | {:error, Ecto.Changeset.t()}
  def update_team(%Team{} = team, attrs) do
    team
    |> Team.changeset(attrs)
    |> Repo.update()
  end

  @doc "Deletes a Team"
  @spec delete_team(Team.t()) :: {:ok, Team.t()} | {:error, Ecto.Changeset.t()}
  def delete_team(%Team{} = team), do: Repo.delete(team)

  @doc "Returns an `%Ecto.Changeset{}` for tracking team changes"
  @spec change_team(Team.t()) :: Ecto.Changeset.t()
  def change_team(%Team{} = team), do: Team.changeset(team, %{})

  @doc """
  Lists all the teams a given user is part of.

  ## Examples
  ### User is owner of a team and belongs another user team
  iex(1) > Logflare.Teams.list_teams_by_user_access(user, "token")
  [%Logflare.Team{}, %Logflare.Team{}]
  """
  @spec list_teams_by_user_access(User.t()) :: [Team.t()]
  def list_teams_by_user_access(user) do
    user
    |> query_teams_by_user_access()
    |> Repo.all()
  end

  @doc """
  Fetchesa single team with the given token from the list of teams he's part of

  ## Examples
  ### User is owner of a team and belongs another user team
  iex(1) > Logflare.Teams.get_team_by_user_access(user, "token")
  %Logflare.Team{}
  """
  @spec get_team_by_user_access(User.t(), binary()) :: Team.t() | nil
  def get_team_by_user_access(user, token) do
    user
    |> query_teams_by_user_access()
    |> where([t, tu], t.token == ^token or tu.token == ^token)
    |> Repo.one()
  end

  defp query_teams_by_user_access(%User{id: id, provider_uid: uid}) do
    from t in Team,
      left_join: tu in TeamUser,
      on: t.id == tu.team_id,
      where: t.user_id == ^id or tu.provider_uid == ^uid,
      distinct: true,
      preload: [:user, :team_users],
      select: t
  end
end
