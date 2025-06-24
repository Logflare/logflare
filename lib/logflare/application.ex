defmodule Logflare.Application do
  @moduledoc false
  use Application
  require Logger

  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.ContextCache
  alias Logflare.Logs
  alias Logflare.SingleTenant
  alias Logflare.SystemMetricsSup
  alias Logflare.Sources.Counters
  alias Logflare.Sources.RateCounters
  alias Logflare.PubSubRates

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

    if Application.get_env(:logflare, :opentelemetry_enabled?) do
      OpentelemetryBandit.setup()
      OpentelemetryPhoenix.setup(adapter: :bandit)
    end

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Logflare.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp get_children(:test) do
    finch_pools() ++
      [
        Logflare.Repo,
        Logflare.Vault,
        ContextCache.Supervisor,
        Counters,
        RateCounters,
        Logs.LogEvents.Cache,
        {Phoenix.PubSub, name: Logflare.PubSub},
        PubSubRates,
        Logs.RejectedLogEvents,
        Logflare.Backends,
        {Registry,
         name: Logflare.V1SourceRegistry,
         keys: :unique,
         partitions: max(round(System.schedulers_online() / 8), 1)},
        {PartitionSupervisor, child_spec: Task.Supervisor, name: Logflare.TaskSupervisors},
        {PartitionSupervisor,
         child_spec: DynamicSupervisor, name: Logflare.Endpoints.Cache.PartitionSupervisor},
        {DynamicSupervisor,
         strategy: :one_for_one,
         restart: :transient,
         max_restarts: 10,
         max_seconds: 60,
         name: Logflare.Source.V1SourceDynSup},
        LogflareWeb.Endpoint
      ]
  end

  defp get_children(_) do
    topologies = Application.get_env(:libcluster, :topologies, [])
    grpc_port = Application.get_env(:grpc, :port)
    ssl = Application.get_env(:logflare, :ssl)
    grpc_creds = if ssl, do: GRPC.Credential.new(ssl: ssl)
    pool_size = Application.get_env(:logflare, Logflare.PubSub)[:pool_size]

    # set goth early in the supervision tree
    finch_pools() ++
      conditional_children() ++
      [
        Logflare.ErlSysMon,
        {PartitionSupervisor, child_spec: Task.Supervisor, name: Logflare.TaskSupervisors},
        {Cluster.Supervisor, [topologies, [name: Logflare.ClusterSupervisor]]},
        Logflare.Repo,
        Logflare.Vault,
        {Phoenix.PubSub, name: Logflare.PubSub, pool_size: pool_size},
        ContextCache.Supervisor,
        Logs.LogEvents.Cache,
        PubSubRates,

        # v1 ingest pipline
        {Registry,
         name: Logflare.V1SourceRegistry,
         keys: :unique,
         partitions: max(round(System.schedulers_online() / 8), 1)},
        Logs.RejectedLogEvents,
        # init Counters before Supervisof as Supervisor calls Counters through table create
        Counters,
        RateCounters,
        # Backends needs to be before Source.Supervisor
        Logflare.Backends,
        Logflare.Source.Supervisor,
        {DynamicSupervisor,
         strategy: :one_for_one,
         restart: :transient,
         max_restarts: 10,
         max_seconds: 60,
         name: Logflare.Source.V1SourceDynSup},
        LogflareWeb.Endpoint,
        # If we get a log event and the Source.Supervisor is not up it will 500
        {GRPC.Server.Supervisor,
         endpoint: LogflareGrpc.Endpoint, port: grpc_port, cred: grpc_creds, start_server: true},
        # Monitor system level metrics
        SystemMetricsSup,
        Logflare.Telemetry,

        # For Logflare Endpoints
        {PartitionSupervisor,
         child_spec: DynamicSupervisor, name: Logflare.Endpoints.Cache.PartitionSupervisor},

        # Startup tasks after v2 pipeline started
        {Task, fn -> startup_tasks() end},

        # citrine scheduler for alerts
        Logflare.Alerting.Supervisor
      ]
  end

  def goth_partition_count, do: 5

  def conditional_children do
    goth =
      [
        BigQueryAdaptor.partitioned_goth_child_spec()
      ] ++ BigQueryAdaptor.impersonated_goth_child_specs()

    # only add in config cat to multi-tenant prod
    config_cat =
      case Application.get_env(:logflare, :config_cat_sdk_key) do
        nil -> []
        config_cat_key -> [{ConfigCat, [sdk_key: config_cat_key]}]
      end

    goth ++ config_cat
  end

  def config_change(changed, _new, removed) do
    LogflareWeb.Endpoint.config_change(changed, removed)
    :ok
  end

  defp finch_pools do
    base = System.schedulers_online()
    min_count = max(5, ceil(base / 10))
    http1_count = max(div(base, 4), 1)

    [
      # Finch connection pools, using http2
      {Finch, name: Logflare.FinchGoth, pools: %{default: [protocols: [:http2], count: 1]}},
      {Finch,
       name: Logflare.FinchIngest,
       pools: %{
         :default => [size: 50],
         "https://bigquery.googleapis.com" => [
           protocols: [:http1],
           size: max(base * 125, 150),
           count: http1_count,
           start_pool_metrics?: true
         ]
       }},
      {Finch,
       name: Logflare.FinchQuery,
       pools: %{
         "https://bigquery.googleapis.com" => [
           protocols: [:http2],
           count: max(base, 20) * 2,
           start_pool_metrics?: true
         ]
       }},
      {Finch,
       name: Logflare.FinchDefault,
       pools: %{
         # default pool uses finch defaults
         :default => [protocols: [:http1]],
         #  explicitly set http2 for other pools for multiplexing
         "https://bigquery.googleapis.com" => [
           protocols: [:http1],
           size: 100,
           count: http1_count,
           start_pool_metrics?: true
         ],
         "https://http-intake.logs.datadoghq.com" => [
           protocols: [:http2],
           count: min_count,
           start_pool_metrics?: true
         ],
         "https://http-intake.logs.us3.datadoghq.com" => [
           protocols: [:http2],
           count: min_count,
           start_pool_metrics?: true
         ],
         "https://http-intake.logs.us5.datadoghq.com" => [
           protocols: [:http2],
           count: min_count,
           start_pool_metrics?: true
         ],
         "https://http-intake.logs.datadoghq.eu" => [
           protocols: [:http2],
           count: min_count,
           start_pool_metrics?: true
         ],
         "https://http-intake.logs.ap1.datadoghq.com" => [
           protocols: [:http2],
           count: min_count,
           start_pool_metrics?: true
         ]
       }},
      {Finch,
       name: Logflare.FinchDefaultHttp1, pools: %{default: [protocols: [:http1], size: 50]}}
    ]
  end

  def startup_tasks do
    # if single tenant, insert enterprise user
    Logger.info("Executing startup tasks")

    if !SingleTenant.postgres_backend?() do
      BigQueryAdaptor.create_managed_service_accounts()
      BigQueryAdaptor.update_iam_policy()
    end

    if SingleTenant.single_tenant?() do
      Logger.info("Ensuring single tenant user is seeded...")
      SingleTenant.create_default_plan()
      SingleTenant.create_default_user()
      SingleTenant.create_access_tokens()
    end

    if SingleTenant.supabase_mode?() do
      SingleTenant.create_supabase_sources()
      SingleTenant.create_supabase_endpoints()
      SingleTenant.ensure_supabase_sources_started()

      unless SingleTenant.postgres_backend?() do
        # buffer time for all sources to init and create tables
        # in case of latency.
        :timer.sleep(3_000)

        SingleTenant.update_supabase_source_schemas()
      end
    end
  end
end
