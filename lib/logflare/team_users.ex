defmodule Logflare.TeamUsers do
  @moduledoc """
  The TeamUsers context.
  """
  import Ecto.Query, warn: false

  alias Logflare.Teams
  alias Logflare.Teams.Team
  alias Logflare.Repo
  alias Logflare.TeamUsers.TeamUser

  @doc """
  Returns the list of team_users.

  ## Examples

      iex> list_team_users()
      [%TeamUser{}, ...]

  """
  def list_team_users do
    Repo.all(TeamUser)
  end

  def list_team_users_by_and_preload(kv) do
    query =
      from t in TeamUser,
        where: ^kv,
        select: t

    for team_user <- Repo.all(query) do
      Repo.preload(team_user, :team)
    end
  end

  @doc """
  Gets a single team_user.

  Raises `Ecto.NoResultsError` if the Team user does not exist.

  ## Examples

      iex> get_team_user!(123)
      %TeamUser{}

      iex> get_team_user!(456)
      ** (Ecto.NoResultsError)

  """
  def get_team_user_by(kv), do: Repo.get_by(TeamUser, kv)

  def get_team_user_by_team_provider_uid(team_id, provider_uid) do
    from(u in TeamUser, where: u.team_id == ^team_id and u.provider_uid == ^provider_uid)
    |> Repo.one()
  end

  def get_team_user!(id), do: Repo.get!(TeamUser, id)

  def get_team_user_and_preload(id) do
    case Repo.get(TeamUser, id) do
      nil ->
        nil

      team_user ->
        Repo.preload(team_user, :team)
    end
  end

  @doc """
  Creates a team_user.

  ## Examples

      iex> create_team_user(%{field: value})
      {:ok, %TeamUser{}}

      iex> create_team_user(%{field: bad_value})
      {:error, %Ecto.Changeset{}}

  """
  def create_team_user(team_id, team_user_attrs \\ %{}) do
    Teams.get_team!(team_id)
    |> Ecto.build_assoc(:team_users)
    |> TeamUser.changeset(team_user_attrs)
    |> Repo.insert()
  end

  @doc """
  Updates a team_user.

  ## Examples

      iex> update_team_user(team_user, %{field: new_value})
      {:ok, %TeamUser{}}

      iex> update_team_user(team_user, %{field: bad_value})
      {:error, %Ecto.Changeset{}}

  """
  def update_team_user(%TeamUser{} = team_user, attrs) do
    team_user
    |> TeamUser.changeset(attrs)
    |> Repo.update()
  end

  @doc """
  Deletes a TeamUser.

  ## Examples

      iex> delete_team_user(team_user)
      {:ok, %TeamUser{}}

      iex> delete_team_user(team_user)
      {:error, %Ecto.Changeset{}}

  """
  def delete_team_user(%TeamUser{} = team_user) do
    Repo.delete(team_user)
  end

  @doc """
  Returns an `%Ecto.Changeset{}` for tracking team_user changes.

  ## Examples

      iex> change_team_user(team_user)
      %Ecto.Changeset{source: %TeamUser{}}

  """
  def change_team_user(%TeamUser{} = team_user) do
    TeamUser.changeset(team_user, %{})
  end
end
