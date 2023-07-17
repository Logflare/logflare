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
    # TODO: Set node status in GCP when sigterm is received
    :ok =
      :gen_event.swap_sup_handler(
        :erl_signal_server,
        {:erl_signal_handler, []},
        {Logflare.SigtermHandler, []}
      )

    children = get_children(env)

    opts = [strategy: :one_for_one, name: Logflare.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def config_change(changed, _new, removed) do
    LogflareWeb.Endpoint.config_change(changed, removed)
    :ok
  end

  def start_phase(:seed_system, _, env: :test), do: :ok

  def start_phase(:seed_system, _, _) do
    startup_tasks()
    :ok
  end

  defp get_children(:test) do
    cache_children() ++
      [
        Logflare.Repo,
        Logs.RejectedLogEvents,
        {Phoenix.PubSub, name: Logflare.PubSub, pool_size: 10},
        {Task.Supervisor, name: Logflare.TaskSupervisor}
      ] ++
      v2_ingestion_pipeline_children() ++
      common_children()
  end

  defp get_children(_) do
    # Database options for Postgres notifications

    topologies = Application.get_env(:libcluster, :topologies, [])

    cache_children() ++
      [
        Logflare.Repo,
        {Task.Supervisor, name: Logflare.TaskSupervisor},
        {Cluster.Supervisor, [topologies, [name: Logflare.ClusterSupervisor]]},
        Logs.RejectedLogEvents,
        Sources.Counters,
        Sources.RateCounters,
        {Phoenix.PubSub, name: Logflare.PubSub, pool_size: 10},
        PubSubRates.Rates,
        PubSubRates.Buffers,
        PubSubRates.Inserts,
        Logflare.Source.Supervisor,

        # If we get a log event and the Source.Supervisor is not up it will 500
        # Monitor system level metrics
        Logflare.SystemMetricsSup
      ] ++
      get_goth_children() ++
      replication_log_children() ++
      v2_ingestion_pipeline_children() ++
      grpc_children() ++
      conditional_children() ++
      common_children()
  end

  defp replication_log_children() do
    hostname = '#{Application.get_env(:logflare, Logflare.Repo)[:hostname]}'
    username = Application.get_env(:logflare, Logflare.Repo)[:username]
    password = Application.get_env(:logflare, Logflare.Repo)[:password]
    database = Application.get_env(:logflare, Logflare.Repo)[:database]
    port = Application.get_env(:logflare, Logflare.Repo)[:port]
    slot = Application.get_env(:logflare, Logflare.CacheBuster)[:replication_slot]
    publications = Application.get_env(:logflare, Logflare.CacheBuster)[:publications]

    opts = [
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
    ]

    [{Cainophile.Adapters.Postgres, opts}, Logflare.CacheBuster]
  end

  defp v2_ingestion_pipeline_children() do
    [
      {DynamicSupervisor, strategy: :one_for_one, name: Logflare.Backends.SourcesSup},
      {DynamicSupervisor, strategy: :one_for_one, name: Logflare.Backends.RecentLogsSup},
      {DynamicSupervisor,
       strategy: :one_for_one, name: Logflare.Backends.Adaptor.PostgresAdaptor.Supervisor},
      {Registry, name: Logflare.Backends.SourceRegistry, keys: :unique},
      {Registry, name: Logflare.Backends.SourceDispatcher, keys: :duplicate}
    ]
  end

  defp conditional_children do
    config_cat_key = Application.get_env(:logflare, :config_cat_sdk_key)

    # only add in config cat to multi-tenant prod
    if(config_cat_key, do: [{ConfigCat, [sdk_key: config_cat_key]}], else: [])
  end

  defp common_children do
    [
      # Finch connection pools, using http2
      {Finch, name: Logflare.FinchIngest, pools: %{:default => [protocol: :http2, count: 200]}},
      {Finch, name: Logflare.FinchQuery, pools: %{:default => [protocol: :http2, count: 100]}},
      {Finch, name: Logflare.FinchDefault, pools: %{:default => [protocol: :http2, count: 50]}},
      LogflareWeb.Endpoint
    ]
  end

  defp get_goth_children() do
    # Setup Goth for GCP connections
    case Application.get_env(:logflare, :supabase_mode) do
      true ->
        []

      false ->
        credentials = Jason.decode!(Application.get_env(:goth, :json))
        scopes = ["https://www.googleapis.com/auth/cloud-platform"]
        source = {:service_account, credentials, scopes: scopes}
        [{Goth, name: Logflare.Goth, source: source}]
    end
  end

  defp cache_children() do
    [
      ContextCache,
      Users.Cache,
      Sources.Cache,
      Billing.Cache,
      SourceSchemas.Cache,
      PubSubRates.Cache,
      Logs.LogEvents.Cache,
      {DynamicSupervisor, strategy: :one_for_one, name: Logflare.Endpoints.Cache}
    ]
  end

  defp grpc_children() do
    grpc_port = Application.get_env(:grpc, :port)
    ssl = Application.get_env(:logflare, :ssl)
    grpc_creds = if ssl, do: GRPC.Credential.new(ssl: ssl)

    [{GRPC.Server.Supervisor, {LogflareGrpc.Endpoint, grpc_port, cred: grpc_creds}}]
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
      SingleTenant.ensure_supabase_sources_started()
      # buffer time for all sources to init and create tables
      # in case of latency.
      :timer.sleep(3_000)
    end
  end
end
