defmodule Logflare.SingleTenant do
  @moduledoc """
  Handles single tenant-related logic
  """
  alias Logflare.Users
  alias Logflare.Billing
  alias Logflare.Billing.Plan
  alias Logflare.Endpoints.Query
  alias Logflare.Source
  alias Logflare.Sources
  alias Logflare.Endpoints
  alias Logflare.Repo
  alias Logflare.Source.Supervisor
  alias Logflare.Source.BigQuery.Schema
  alias Logflare.LogEvent
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
      name: "logs.all",
      query:
        Application.app_dir(:logflare, "priv/supabase/endpoints/logs.all.sql") |> File.read!(),
      sandboxable: true,
      max_limit: 1000,
      enable_auth: false
    },
    %{
      name: "charts.usage",
      query:
        Application.app_dir(:logflare, "priv/supabase/endpoints/charts.usage.sql") |> File.read!(),
      sandboxable: true,
      max_limit: 1000,
      enable_auth: false
    },
    %{
      name: "functions.invocation-stats",
      query:
        Application.app_dir(:logflare, "priv/supabase/endpoints/functions.invocation-stats.sql")
        |> File.read!(),
      sandboxable: true,
      max_limit: 1000,
      enable_auth: false
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
    |> Enum.find(fn plan ->
      @plan_attrs = plan
    end)
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
    plan =
      Billing.list_plans()
      |> Enum.find(fn plan -> plan.name == "Enterprise" end)

    if plan == nil do
      Billing.create_plan(@plan_attrs)
    else
      {:error, :already_created}
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
          {:ok, source} = Sources.create_source(%{name: name}, user)

          Supervisor.ensure_started(source.token)
          source
        end

      {:ok, sources}
    else
      {:error, :already_created}
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
  Adds ingestion samples for supabase sources, so that schema is built and stored correctly.
  """
  @spec update_supabase_source_schemas :: nil
  def update_supabase_source_schemas do
    if supabase_mode?() do
      user = get_default_user()

      sources =
        Sources.list_sources_by_user(user)
        |> Repo.preload(:rules)

      tasks =
        for source <- sources do
          Task.async(fn ->
            source = Sources.refresh_source_metrics_for_ingest(source)
            Logger.debug("Updating schemas for for #{source.name}")
            event = read_ingest_sample_json(source.name)
            log_event = LogEvent.make(event, %{source: source})
            Schema.update(source.token, log_event)
          end)
        end

      Task.await_many(tasks)
    end
  end

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
      if default_user do
        if Sources.list_sources_by_user(default_user) |> length() > 0, do: :ok
      end

    seed_endpoints =
      if default_user do
        if Endpoints.list_endpoints_by(user_id: default_user.id) |> length() > 0, do: :ok
      end

    source_schemas_updated = if supabase_mode_source_schemas_updated?(), do: :ok

    %{
      seed_user: seed_user,
      seed_plan: seed_plan,
      seed_sources: seed_sources,
      seed_endpoints: seed_endpoints,
      source_schemas_updated: source_schemas_updated
    }
  end

  def supabase_mode_source_schemas_updated? do
    user = get_default_user()

    if user do
      sources = Sources.list_sources_by_user(user)

      checks =
        for source <- sources,
            source.name in @source_names,
            state = Schema.get_state(source.token) do
          state.field_count > 3
        end

      Enum.all?(checks) and length(sources) > 0
    else
      false
    end
  end

  # Read a source ingest sample json file
  defp read_ingest_sample_json(source_name) do
    Application.app_dir(:logflare, "priv/supabase/ingest_samples/#{source_name}.json")
    |> File.read!()
    |> Jason.decode!()
  end
end
