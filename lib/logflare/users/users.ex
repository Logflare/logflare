defmodule Logflare.Users do
  use Logflare.Commons
  alias Logflare.Google.BigQuery
  alias Logflare.Google.CloudResourceManager
  alias Logflare.Source.Supervisor

  @moduledoc false

  def get_user(user_id) do
    get_by(User, id: user_id)
  end

  def get_user!(user_id), do: get_by!(User, id: user_id)

  def get_user_by(keyword) do
    get_by(User, keyword)
  end

  def get_by_and_preload(keyword) do
    User
    |> RepoWithCache.get_by(keyword)
    |> case do
      %User{} = u -> preload_defaults(u)
      nil -> nil
    end
  end

  def preload_team(user) do
    user
    |> RepoWithCache.preload(team: :team_users)
  end

  def preload_billing_account(user) do
    user
    |> RepoWithCache.preload(:billing_account)
  end

  def preload_defaults(user) do
    user
    |> preload_sources
    |> preload_team()
    |> maybe_preload_bigquery_defaults()
  end

  def preload_sources(user) do
    user
    |> RepoWithCache.preload(:sources)
  end

  def maybe_preload_bigquery_defaults(user) do
    user =
      if is_nil(user.bigquery_dataset_id) do
        %{user | bigquery_dataset_id: User.generate_bq_dataset_id(user)}
      else
        user
      end

    if is_nil(user.bigquery_project_id) do
      %{user | bigquery_project_id: BigQuery.GCPConfig.default_project_id()}
    else
      user
    end
  end

  def get_by_source(source_id) when is_atom(source_id) do
    %Logflare.Source{user_id: user_id} = Sources.get_by(token: source_id)
    Users.get_by_and_preload(id: user_id)
  end

  def update_user_all_fields(user, params) do
    user
    |> User.changeset(params)
    |> RepoWithCache.update()
  end

  def update_user_allowed(user, params) do
    user
    |> User.user_allowed_changeset(params)
    |> RepoWithCache.update()
  end

  def insert_or_update_user(auth_params) do
    cond do
      user = RepoWithCache.get_by(User, provider_uid: auth_params.provider_uid) ->
        update_user_by_provider_id(user, auth_params)

      user = RepoWithCache.get_by(User, email: auth_params.email) ->
        update_user_by_email(user, auth_params)

      true ->
        api_key = :crypto.strong_rand_bytes(12) |> Base.url_encode64() |> binary_part(0, 12)
        auth_params = Map.put(auth_params, :api_key, api_key)

        changeset = User.changeset(%User{}, auth_params)
        RepoWithCache.insert(changeset)
    end
  end

  def delete_user(user) do
    Supervisor.delete_all_user_sources(user)

    case RepoWithCache.delete(user) do
      {:ok, _user} = response ->
        BigQuery.delete_dataset(user)
        CloudResourceManager.set_iam_policy()
        response

      {:error, _reason} = response ->
        response
    end
  end

  defp update_user_by_email(user, auth_params) do
    updated_changeset = User.changeset(user, auth_params)

    case RepoWithCache.update(updated_changeset) do
      {:ok, user} ->
        {:ok_found_user, user}

      {:error, changeset} ->
        {:error, changeset}
    end
  end

  defp update_user_by_provider_id(user, auth_params) do
    updated_changeset = User.changeset(user, auth_params)

    case RepoWithCache.update(updated_changeset) do
      {:ok, user} ->
        {:ok_found_user, user}

      {:error, changeset} ->
        {:error, changeset}
    end
  end

  def change_user_preferences(user_preferences, attrs \\ %{}) do
    (user_preferences || %UserPreferences{})
    |> UserPreferences.changeset(attrs)
  end

  def update_user_with_preferences(user_or_team_user, attrs)
      when user_or_team_user.__struct__ in [User, TeamUser] do
    user_or_team_user
    |> Ecto.Changeset.cast(attrs, [])
    |> Ecto.Changeset.cast_embed(:preferences, required: true)
    |> RepoWithCache.update()
  end

  defp get_by(schema, kw), do: RepoWithCache.get_by(schema, kw)
  defp get_by!(schema, kw), do: RepoWithCache.get_by!(schema, kw)
end
