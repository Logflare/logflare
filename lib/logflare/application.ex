defmodule Logflare.Application do
  @moduledoc false
  use Application
  require Logger

  alias Logflare.Billing
  alias Logflare.ContextCache
  alias Logflare.Logs
  alias Logflare.PubSubRates
  alias Logflare.SingleTenant
  alias Logflare.Sources
  alias Logflare.SourceSchemas
  alias Logflare.Users

  def start(_type, _args) do
    env = Application.get_env(:logflare, :env)
    ensure_loaded_postgres_backend_migrations()
    # TODO: Set node status in GCP when sigterm is received
    :ok =
      :gen_event.swap_sup_handler(
        :erl_signal_server,
        {:erl_signal_handler, []},
        {Logflare.SigtermHandler, []}
      )

    children = get_children(env)

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Logflare.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp get_goth_child_spec() do
    # Setup Goth for GCP connections
    require Logger
    credentials = Jason.decode!(Application.get_env(:goth, :json))
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    source = {:service_account, credentials, scopes: scopes}
    {Goth, name: Logflare.Goth, source: source}
  end

  defp get_children(:test) do
    [
      ContextCache,
      Users.Cache,
      Sources.Cache,
      Billing.Cache,
      SourceSchemas.Cache,
      PubSubRates.Cache,
      Logs.LogEvents.Cache,
      Logs.RejectedLogEvents,
      {Phoenix.PubSub, name: Logflare.PubSub},
      Logflare.Repo,
      # get_goth_child_spec(),
      LogflareWeb.Endpoint,
      {Task.Supervisor, name: Logflare.TaskSupervisor},
      {DynamicSupervisor, strategy: :one_for_one, name: Logflare.Endpoints.Cache},
      # v2 ingestion pipelines
      {DynamicSupervisor, strategy: :one_for_one, name: Logflare.Backends.SourcesSup},
      {DynamicSupervisor, strategy: :one_for_one, name: Logflare.Backends.RecentLogsSup},
      {DynamicSupervisor,
       strategy: :one_for_one, name: Logflare.Backends.Adaptor.Postgres.Supervisor},
      {Registry, name: Logflare.Backends.SourceRegistry, keys: :unique},
      {Registry, name: Logflare.Backends.SourceDispatcher, keys: :duplicate}
    ] ++ common_children()
  end

  defp get_children(_) do
    # Database options for Postgres notifications
    hostname = '#{Application.get_env(:logflare, Logflare.Repo)[:hostname]}'
    username = Application.get_env(:logflare, Logflare.Repo)[:username]
    password = Application.get_env(:logflare, Logflare.Repo)[:password]
    database = Application.get_env(:logflare, Logflare.Repo)[:database]

    port = Application.get_env(:logflare, Logflare.Repo)[:port]
    slot = Application.get_env(:logflare, Logflare.CacheBuster)[:replication_slot]
    publications = Application.get_env(:logflare, Logflare.CacheBuster)[:publications]
    topologies = Application.get_env(:libcluster, :topologies, [])
    grpc_port = Application.get_env(:grpc, :port)
    ssl = Application.get_env(:logflare, :ssl)
    grpc_creds = if ssl, do: GRPC.Credential.new(ssl: ssl)

    [
      {Task.Supervisor, name: Logflare.TaskSupervisor},
      {Cluster.Supervisor, [topologies, [name: Logflare.ClusterSupervisor]]},
      get_goth_child_spec(),
      Logflare.Repo,
      {Phoenix.PubSub, name: Logflare.PubSub, pool_size: 10},
      # supervisor(LogflareTelemetry.Supervisor, []),
      # Context Caches
      ContextCache,
      Users.Cache,
      Sources.Cache,
      Billing.Cache,
      SourceSchemas.Cache,
      PubSubRates.Cache,
      Logs.LogEvents.Cache,

      # Follow Postgresql replication log and bust all our context caches
      {
        Cainophile.Adapters.Postgres,
        register: Logflare.PgPublisher,
        epgsql: %{
          host: hostname,
          port: port,
          username: username,
          database: database,
          password: password
        },
        slot: slot,
        wal_position: {"0", "0"},
        publications: publications
      },
      Logflare.CacheBuster,

      # Sources
      Logs.RejectedLogEvents,
      # init Counters before Supervisof as Supervisor calls Counters through table create
      Sources.Counters,
      Sources.RateCounters,
      PubSubRates.Rates,
      PubSubRates.Buffers,
      PubSubRates.Inserts,
      Logflare.Source.Supervisor,

      # If we get a log event and the Source.Supervisor is not up it will 500
      LogflareWeb.Endpoint,
      {GRPC.Server.Supervisor, {LogflareGrpc.Endpoint, grpc_port, cred: grpc_creds}},
      # Monitor system level metrics
      Logflare.SystemMetricsSup,

      # For Logflare Endpoints
      {DynamicSupervisor, strategy: :one_for_one, name: Logflare.Endpoints.Cache},

      # Startup tasks
      {Task, fn -> startup_tasks() end},

      # v2 ingestion pipelines
      {DynamicSupervisor, strategy: :one_for_one, name: Logflare.Backends.SourcesSup},
      {DynamicSupervisor, strategy: :one_for_one, name: Logflare.Backends.RecentLogsSup},
      {DynamicSupervisor,
       strategy: :one_for_one, name: Logflare.Backends.Adaptor.Postgres.Supervisor},
      {Registry, name: Logflare.Backends.SourceRegistry, keys: :unique},
      {Registry, name: Logflare.Backends.SourceDispatcher, keys: :duplicate}
    ] ++ conditional_children() ++ common_children()
  end

  def conditional_children do
    config_cat_key = Application.get_env(:logflare, :config_cat_sdk_key)

    # only add in config cat to multi-tenant prod
    if config_cat_key do
      [
        {ConfigCat, [sdk_key: config_cat_key]}
      ]
    else
      []
    end
  end

  def config_change(changed, _new, removed) do
    LogflareWeb.Endpoint.config_change(changed, removed)
    :ok
  end

  defp common_children do
    [
      # Finch connection pools, using http2
      {Finch, name: Logflare.FinchIngest, pools: %{:default => [protocol: :http2, count: 200]}},
      {Finch, name: Logflare.FinchQuery, pools: %{:default => [protocol: :http2, count: 100]}},
      {Finch, name: Logflare.FinchDefault, pools: %{:default => [protocol: :http2, count: 50]}}
    ]
  end

  def startup_tasks do
    # if single tenant, insert enterprise user
    Logger.info("Executing startup tasks")

    if SingleTenant.single_tenant?() do
      Logger.info("Ensuring single tenant user is seeded...")
      SingleTenant.create_default_plan()
      SingleTenant.create_default_user()
    end

    if SingleTenant.supabase_mode?() do
      SingleTenant.create_supabase_sources()
      SingleTenant.create_supabase_endpoints()
      # buffer time for all sources to init and create tables
      # in case of latency.
      :timer.sleep(3_000)
      SingleTenant.update_supabase_source_schemas()
    end
  end

  defp ensure_loaded_postgres_backend_migrations do
    # Needed to ensure that the migrations are properly loaded
    # This was found during tests where the use Ecto.Migration apparently wasn't
    # loading the migrations properly, triggering errors when running Ecto.Migrator

    # Similar error found in Realtime: https://github.com/supabase/realtime/pull/520/files#diff-1de8846d5d70df4b816a1b2bca51468d6c8386bc81f9efe82df2bf837367497d
    Logflare.Backends.Adaptor.Postgres.Repo.migrations()
    |> Enum.map(fn {_, migration} -> Code.ensure_loaded!(migration) end)
  end
end
