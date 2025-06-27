defmodule Logflare.Users do
  require Logger

  import Ecto.Query
  alias Logflare.Google.BigQuery
  alias Logflare.Google.CloudResourceManager
  alias Logflare.Repo
  alias Logflare.Source.Supervisor
  alias Logflare.Sources
  alias Logflare.TeamUsers.TeamUser
  alias Logflare.User
  alias Logflare.Users
  alias Logflare.Users.UserPreferences

  @max_limit 100

  @moduledoc false
  def user_changeset(user, attrs) do
    User.changeset(user, attrs)
  end

  @spec count_users() :: integer()
  def count_users do
    Repo.aggregate(User, :count)
  end

  @doc "Lists users with sources that are actively ingesting events"
  @spec list_ingesting_users(keyword()) :: [User.t()]
  def list_ingesting_users(limit: limit) do
    from(u in User,
      join: s in assoc(u, :sources),
      where: s.log_events_updated_at >= ago(1, "day"),
      order_by: {:desc, s.log_events_updated_at},
      limit: ^limit,
      select: u
    )
    |> Repo.all()
  end

  @doc """
  Lists users and performs filtering based on filter keywords.

  Filters:
  - `metadata`: filter on each key-value pair provided, each additional is considered an AND.
  - `partner_id`: filters down to users of that partner.

  Options:
  - `limit`: max returned users. Defaults to #{@max_limit}

  """
  @spec list_users(keyword()) :: [User.t()]
  def list_users(kw) do
    {opts, filters} =
      Enum.into(kw, %{
        limit: @max_limit
      })
      |> Map.split([:limit])

    filters
    |> Enum.reduce(from(u in User), fn
      {:partner_id, id}, q when is_integer(id) ->
        q
        |> where([u], u.partner_id == ^id)

      {:metadata, %{} = filters}, q ->
        Enum.reduce(filters, q, fn {filter_k, v}, acc ->
          normalized_k = if is_atom(filter_k), do: Atom.to_string(filter_k), else: filter_k
          where(acc, [u], fragment("? -> ?", u.metadata, ^normalized_k) == ^v)
        end)

      {:provider, :google}, q ->
        where(q, [u], u.provider == "google" and u.valid_google_account != false)

      {:paying, true}, q ->
        join(q, :left, [u], ba in assoc(u, :billing_account))
        |> where(
          [u, ..., ba],
          (not is_nil(ba.stripe_subscriptions) and
             fragment("jsonb_array_length(? -> 'data')", ba.stripe_subscriptions) > 0) or
            (is_nil(ba) and u.billing_enabled) == false or
            ba.lifetime_plan == true
        )

      _, q ->
        q
    end)
    |> limit(^min(opts.limit, @max_limit))
    |> Repo.all()
    |> Enum.map(&maybe_put_bigquery_defaults/1)
  end

  def get(user_id) do
    Repo.get(User, user_id)
    |> maybe_put_bigquery_defaults()
  end

  def get_by(kw) do
    Repo.get_by(User, kw)
    |> maybe_put_bigquery_defaults()
  end

  def get_by_and_preload(kw) do
    get_by(kw)
    |> preload_defaults()
  end

  def preload_defaults(nil), do: nil

  def preload_defaults(%User{} = user) do
    user
    |> Repo.preload([:sources, :billing_account, :team])
    |> Map.update!(:sources, fn sources ->
      Enum.map(sources, &Sources.put_retention_days/1)
    end)
  end

  def preload_defaults(users) when is_list(users) do
    users
    |> Repo.preload([:sources, :billing_account, :team])
    |> Enum.map(fn user ->
      user
      |> maybe_put_bigquery_defaults()
      |> Map.update!(:sources, fn sources ->
        Enum.map(sources, &Sources.put_retention_days/1)
      end)
    end)
  end

  def preload_team(user) do
    Repo.preload(user, :team)
  end

  def preload_valid_google_team_users(user) do
    query =
      from(tu in TeamUser, where: tu.valid_google_account != false and tu.provider == "google")

    Repo.preload(user, team: [team_users: query])
  end

  def preload_billing_account(user) do
    Repo.preload(user, :billing_account)
  end

  def preload_vercel_auths(user) do
    Repo.preload(user, :vercel_auths)
  end

  def preload_sources(user) do
    Repo.preload(user, :sources)
    |> Map.update!(:sources, fn sources ->
      Enum.map(sources, &Sources.put_retention_days/1)
    end)
  end

  def preload_endpoints(user) do
    Repo.preload(user, :endpoint_queries)
  end

  defp maybe_put_bigquery_defaults(nil), do: nil

  defp maybe_put_bigquery_defaults(user) do
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
