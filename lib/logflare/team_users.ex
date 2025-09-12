defmodule Logflare.TeamUsers do
  @moduledoc """
  The TeamUsers context.
  """
  import Ecto.Query, warn: false

  alias Logflare.Teams
  alias Logflare.Billing
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
        select: t,
        preload: [:team]

    Repo.all(query)
  end

  def list_team_users_by(kv) do
    query =
      from t in TeamUser,
        where: ^kv,
        select: t

    Repo.all(query)
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

  def insert_or_update_team_user(team, auth_params) do
    team_id = team.id
    user = team.user

    cond do
      team_user =
          from(u in TeamUser,
            where: u.team_id == ^team_id and u.provider_uid == ^auth_params.provider_uid
          )
          |> Repo.one() ->
        update_team_user(team_user, auth_params)

      team_user =
          from(u in TeamUser, where: u.team_id == ^team_id and u.email == ^auth_params.email)
          |> Repo.one() ->
        update_team_user(team_user, auth_params)

      true ->
        count = list_team_users_by(id: team_id) |> Enum.count()
        %Billing.Plan{limit_team_users_limit: limit} = Billing.get_plan_by_user(user)

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

  def get_team_user!(id), do: Repo.get!(TeamUser, id)

  def get_team_user(id), do: Repo.get(TeamUser, id)

  def get_team_user_and_preload(id) do
    case Repo.get(TeamUser, id) do
      nil ->
        nil

      team_user ->
        Repo.preload(team_user, :team)
    end
  end

  def preload_defaults(team_user) do
    team_user
    |> Repo.preload(:team)
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

  def touch_team_user(%TeamUser{} = team_user) do
    from(t in TeamUser, select: t, where: t.id == ^team_user.id)
    |> Repo.update_all(set: [updated_at: DateTime.utc_now()])
  end

  @doc """
  Removes a team user, ensuring `user` is the team owner.

  ## Examples

      iex> remove_team_user(user, team_user_id)
      {:ok, %TeamUser{}}

      iex> remove_team_user(user, team_user_id)
      {:error, :not_authorized}

      iex> remove_team_user(user, team_user_id)
      {:error, :not_found}

  """
  @spec remove_team_user(Logflare.User.t(), non_neg_integer()) ::
          {:ok, TeamUser.t()} | {:error, :not_authorized | :not_found | Ecto.Changeset.t()}
  def remove_team_user(current_user, team_user_id) do
    with team_user when is_struct(team_user) <- get_team_user_and_preload(team_user_id),
         true <- team_user.team.user_id == current_user.id,
         {:ok, team_user} <- delete_team_user(team_user) do
      Logflare.Backends.Adaptor.BigQueryAdaptor.update_iam_policy()
      Logflare.Backends.Adaptor.BigQueryAdaptor.patch_dataset_access(current_user)
      {:ok, team_user}
    else
      nil -> {:error, :not_found}
      false -> {:error, :not_authorized}
      error -> error
    end
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
