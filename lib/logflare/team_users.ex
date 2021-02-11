defmodule Logflare.TeamUsers do
  @moduledoc """
  The TeamUsers context.
  """
  import Ecto.Query, warn: false

  use Logflare.Commons

  @doc """
  Returns the list of team_users.

  ## Examples

      iex> list_team_users()
      [%TeamUser{}, ...]

  """
  def list_team_users do
    RepoWithCache.all(TeamUser)
  end

  def list_team_users_by_and_preload(kv) do
    query =
      from t in TeamUser,
        where: ^kv,
        select: t

    for team_user <- RepoWithCache.all(query) do
      RepoWithCache.preload(team_user, :team)
    end
  end

  def list_team_users_by(kv) do
    query =
      from t in TeamUser,
        where: ^kv,
        select: t

    RepoWithCache.all(query)
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
  def get_team_user_by(kv), do: RepoWithCache.get_by(TeamUser, kv)

  def insert_or_update_team_user(team, auth_params) do
    team_id = team.id
    user = team.user

    cond do
      team_user =
          from(u in TeamUser,
            where: u.team_id == ^team_id and u.provider_uid == ^auth_params.provider_uid
          )
          |> RepoWithCache.one() ->
        update_team_user(team_user, auth_params)

      team_user =
          from(u in TeamUser, where: u.team_id == ^team_id and u.email == ^auth_params.email)
          |> RepoWithCache.one() ->
        update_team_user(team_user, auth_params)

      true ->
        count = list_team_users_by(id: team_id) |> Enum.count()
        %Plans.Plan{limit_team_users_limit: limit} = Plans.get_plan_by_user(user)

        if count < limit do
          create_team_user(team_id, auth_params)
        else
          {:error, :limit_reached}
        end
    end
  end

  def update_team_user_on_change_team(user, team_user_id) do
    team_user = get_team_user(team_user_id)

    {:ok, _team_user} =
      update_team_user(team_user, %{
        provider: user.provider,
        valid_google_account: user.valid_google_account || false,
        token: user.token,
        image: user.image,
        provider_uid: user.provider_uid
      })
  end

  def get_team_user!(id), do: RepoWithCache.get!(TeamUser, id)

  def get_team_user(id), do: RepoWithCache.get(TeamUser, id)

  def get_team_user_and_preload(id) do
    case RepoWithCache.get(TeamUser, id) do
      nil ->
        nil

      team_user ->
        RepoWithCache.preload(team_user, :team)
    end
  end

  def preload_defaults(team_user) do
    team_user
    |> RepoWithCache.preload(:team)
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
    |> RepoWithCache.insert()
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
    |> RepoWithCache.update()
  end

  def touch_team_user(%TeamUser{} = team_user) do
    from(t in TeamUser, select: t, where: t.id == ^team_user.id)
    |> RepoWithCache.update_all(set: [updated_at: DateTime.utc_now()])
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
    RepoWithCache.delete(team_user)
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
