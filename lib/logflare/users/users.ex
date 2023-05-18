defmodule Logflare.Users do
  require Logger

  alias Logflare.Google.BigQuery
  alias Logflare.Google.CloudResourceManager
  alias Logflare.Repo
  alias Logflare.Repo
  alias Logflare.Source.Supervisor
  alias Logflare.Sources
  alias Logflare.Sources
  alias Logflare.TeamUsers.TeamUser
  alias Logflare.User
  alias Logflare.Users
  alias Logflare.Users.UserPreferences

  @moduledoc false
  def user_changeset(user, attrs) do
    User.changeset(user, attrs)
  end

  @spec count_users() :: integer()
  def count_users do
    Repo.aggregate(User, :count)
  end

  def list() do
    Repo.all(User)
  end

  def get(user_id) do
    Repo.get(User, user_id)
  end

  def get_by(keyword) do
    Repo.get_by(User, keyword)
  end

  def get_by_and_preload(keyword) do
    user = Repo.get_by(User, keyword)

    case user do
      %User{} = u -> preload_defaults(u)
      nil -> nil
    end
  end

  def preload_team(user) do
    Repo.preload(user, :team)
  end

  def preload_billing_account(user) do
    Repo.preload(user, :billing_account)
  end

  def preload_vercel_auths(user) do
    Repo.preload(user, :vercel_auths)
  end

  def preload_defaults(user) do
    user
    |> preload_sources
    |> maybe_preload_bigquery_defaults()
  end

  def preload_sources(user) do
    Repo.preload(user, :sources)
  end

  def preload_endpoints(user) do
    Repo.preload(user, :endpoint_queries)
  end

  def maybe_preload_bigquery_defaults(user) do
    user =
      case user.bigquery_dataset_id do
        nil -> %{user | bigquery_dataset_id: User.generate_bq_dataset_id(user)}
        _ -> user
      end

    case user.bigquery_project_id do
      nil -> %{user | bigquery_project_id: BigQuery.GCPConfig.default_project_id()}
      _ -> user
    end
  end

  def get_by_source(source_id) when is_atom(source_id) do
    %Logflare.Source{user_id: user_id} = Sources.get_by(token: source_id)
    Users.get_by_and_preload(id: user_id)
  end

  def update_user_all_fields(user, params) do
    user
    |> user_changeset(params)
    |> Repo.update()
  end

  def update_user_allowed(user, params) do
    user
    |> User.user_allowed_changeset(params)
    |> Repo.update()
  end

  @spec insert_user(map()) :: {:ok, User.t()} | {:error, any()}
  def insert_user(params) do
    %User{}
    |> user_changeset(params)
    |> Repo.insert()
  end

  def insert_or_update_user(auth_params)
      when not is_map_key(auth_params, :email) or not is_map_key(auth_params, :provider_uid) do
    {:error, "Missing email or provider_uid"}
  end

  def insert_or_update_user(auth_params) do
    cond do
      user = Repo.get_by(User, provider_uid: auth_params.provider_uid) ->
        update_user_by_provider_id(user, auth_params)

      user = Repo.get_by(User, email: auth_params.email) ->
        update_user_by_email(user, auth_params)

      true ->
        insert_user(auth_params)
    end
  end

  def delete_user(user) do
    Supervisor.delete_all_user_sources(user)

    case Repo.delete(user) do
      {:ok, _user} = response ->
        BigQuery.delete_dataset(user)
        CloudResourceManager.set_iam_policy()
        response

      {:error, _reason} = response ->
        response
    end
  end

  defp update_user_by_email(user, auth_params) do
    updated_changeset = user_changeset(user, auth_params)

    case Repo.update(updated_changeset) do
      {:ok, user} ->
        {:ok_found_user, user}

      {:error, changeset} ->
        {:error, changeset}
    end
  end

  defp update_user_by_provider_id(user, auth_params) do
    updated_changeset = user_changeset(user, auth_params)

    case Repo.update(updated_changeset) do
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
    |> Repo.update()
  end

  def change_owner(%TeamUser{} = team_user, %User{} = user) do
    new_user =
      Map.take(team_user, [
        :email,
        :provider,
        :email_preferred,
        :name,
        :image,
        :phone,
        :valid_google_account,
        :provider_uid
      ])

    update_user_all_fields(user, new_user)
  end
end
