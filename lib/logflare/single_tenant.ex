defmodule Logflare.SingleTenant do
  @moduledoc """
  Handles single tenant-related logic
  """
  alias Logflare.Backends
  alias Logflare.Billing
  alias Logflare.Billing.Plan
  alias Logflare.Endpoints
  alias Logflare.Endpoints.Query
  alias Logflare.Source
  alias Logflare.Repo
  alias Logflare.Sources
  alias Logflare.Users

  require Logger

  @user_attrs %{
    name: "default",
    email: "default@logflare.app",
    email_preferred: "default@logflare.app",
    provider: "default",
    token: Ecto.UUID.generate(),
    provider_uid: "default",
    endpoints_beta: true
  }

  @plan_attrs %{
    name: "Enterprise",
    period: "year",
    price: 20_000,
    limit_sources: 100,
    limit_rate_limit: 5_000,
    limit_alert_freq: 1_000,
    limit_source_rate_limit: 100,
    limit_saved_search_limit: 1,
    limit_team_users_limit: 2,
    limit_source_fields_limit: 500,
    limit_source_ttl: 5_184_000_000,
    type: "standard"
  }

  @source_names [
    "cloudflare.logs.prod",
    "postgres.logs",
    "deno-relay-logs",
    "deno-subhosting-events",
    "gotrue.logs.prod",
    "realtime.logs.prod",
    "storage.logs.prod.2",
    "postgREST.logs.prod",
    "pgbouncer.logs.prod"
  ]

  @endpoint_params [
    %{
      name: "test",
      query: "select body from 'cloudflare.logs.prod'",
      sandboxable: true,
      max_limit: 1000,
      enable_auth: true,
      cache_duration_seconds: 0
    },
    %{
      name: "logs.all",
      query:
        Application.app_dir(:logflare, "priv/supabase/endpoints/logs.all.sql") |> File.read!(),
      sandboxable: true,
      max_limit: 1000,
      enable_auth: true,
      cache_duration_seconds: 0
    },
    %{
      name: "usage.api-counts",
      query:
        Application.app_dir(:logflare, "priv/supabase/endpoints/usage.api-counts.sql")
        |> File.read!(),
      sandboxable: true,
      max_limit: 1000,
      enable_auth: true,
      cache_duration_seconds: 900,
      proactive_requerying_seconds: 300
    },
    %{
      name: "functions.invocation-stats",
      query:
        Application.app_dir(:logflare, "priv/supabase/endpoints/functions.invocation-stats.sql")
        |> File.read!(),
      sandboxable: true,
      max_limit: 1000,
      enable_auth: true,
      cache_duration_seconds: 900,
      proactive_requerying_seconds: 300
    }
  ]

  @doc """
  Retrieves the default user
  """
  def get_default_user do
    Users.get_by(provider_uid: "default", provider: "default")
  end

  @doc """
  Retrieves the default plan
  """
  def get_default_plan do
    Billing.list_plans()
    |> Enum.find(fn plan -> @plan_attrs = plan end)
  end

  @doc """
  Creates an enterprise user
  """
  def create_default_user do
    attrs = Map.put(@user_attrs, :api_key, Application.get_env(:logflare, :api_key))

    case Users.insert_user(attrs) do
      {:ok, _} = result ->
        result

      {:error, %Ecto.Changeset{errors: [email: {_, [{:constraint, :unique} | _]}]}} ->
        {:error, :already_created}
    end
  end

  @doc """
  Creates a default enterprise plan for single tenant
  """
  @spec create_default_plan() :: {:ok, Plan.t()} | {:error, :already_created}
  def create_default_plan do
    plan = Billing.list_plans() |> Enum.find(fn plan -> plan.name == "Enterprise" end)

    case plan do
      nil -> Billing.create_plan(@plan_attrs)
      _ -> {:error, :already_created}
    end
  end

  @doc """
  Inserts a preset number of supabase sources, and ensures that the supervision trees are started and ready for ingestion.
  """
  @spec create_supabase_sources() :: {:ok, [Source.t()]}
  def create_supabase_sources do
    user = get_default_user()
    count = Sources.count_sources_by_user(user)

    if count == 0 do
      sources =
        for name <- @source_names do
          # creating a source will automatically start the source's RLS process
          url = Application.get_env(:logflare, :postgres_backend_url)
          {:ok, source} = Sources.create_source(%{name: name, v2_pipeline: true}, user)
          {:ok, _} = Backends.create_source_backend(source, :postgres, %{url: url})

          source
        end

      {:ok, sources}
    else
      {:error, :already_created}
    end
  end

  @doc """
  Starts supabase sources if present.
  Note: not tested as `Logflare.Source.Supervisor` is a pain to mock.
  TODO: add testing for v2
  """
  @spec ensure_supabase_sources_started() :: list()
  def ensure_supabase_sources_started do
    user = get_default_user()

    if user do
      for source <- Sources.list_sources_by_user(user) do
        source = Repo.preload(source, :source_backends)
        Logflare.Backends.start_source_sup(source)
      end
    end
  end

  @doc """
  Inserts supabase endpoints via SQL files under priv/supabase.any()
  These SQL scripts are directly exported from logflare prod.
  """
  @spec create_supabase_endpoints() :: {:ok, [Query.t()]}
  def create_supabase_endpoints do
    user = get_default_user()
    count = Endpoints.count_endpoints_by_user(user)

    if count == 0 do
      endpoints =
        for params <- @endpoint_params do
          {:ok, endpoint} = Endpoints.create_query(user, params)
          endpoint
        end

      {:ok, endpoints}
    else
      {:error, :already_created}
    end
  end

  @doc "Returns true if single tenant flag is set via config"
  @spec single_tenant? :: boolean()
  def single_tenant?, do: !!Application.get_env(:logflare, :single_tenant)

  @doc "Returns true if supabase mode flag is set via config and if is single tenant"
  @spec supabase_mode? :: boolean()
  def supabase_mode?, do: !!Application.get_env(:logflare, :supabase_mode) and single_tenant?()

  @doc """
  Returns the status of supabase mode setup process.
  Possible statuses: :ok, nil
  """
  @spec supabase_mode_status :: %{atom() => :ok | nil}
  def supabase_mode_status do
    default_plan = get_default_plan()
    default_user = if default_plan, do: get_default_user()
    seed_user = if default_user, do: :ok
    seed_plan = if default_plan, do: :ok

    seed_sources =
      default_user &&
        Sources.list_sources_by_user(default_user)
        |> Enum.map(&Backends.source_sup_started?/1)
        |> Enum.count(& &1)

    seed_endpoints = default_user && Endpoints.list_endpoints_by(user_id: default_user.id)

    %{
      seed_user: seed_user,
      seed_plan: seed_plan,
      seed_sources: if(seed_sources > 0, do: :ok),
      seed_endpoints: if(seed_endpoints > 0, do: :ok)
    }
  end
end
