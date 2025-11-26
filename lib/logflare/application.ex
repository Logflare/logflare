defmodule Logflare.Application do
  @moduledoc false
  use Application

  require Logger

  alias Logflare.Networking
  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Backends.UserMonitoring
  alias Logflare.ContextCache
  alias Logflare.Logs
  alias Logflare.SingleTenant
  alias Logflare.SystemMetricsSup
  alias Logflare.Sources.Counters
  alias Logflare.Sources.RateCounters
  alias Logflare.PubSubRates
  alias Logflare.Utils

  def start(_type, _args) do
    # set inspect function to redact sensitive information
    prev = Inspect.Opts.default_inspect_fun()
    Inspect.Opts.default_inspect_fun(&Utils.inspect_fun(prev, &1, &2))

    start_user_log_interceptor()

    env = Application.get_env(:logflare, :env)
    # TODO: Set node status in GCP when sigterm is received
    :ok =
      :gen_event.swap_sup_handler(
        :erl_signal_server,
        {:erl_signal_handler, []},
        {Logflare.SigtermHandler, []}
      )

    # Routes user-specific logs to their respective system source, when appliable

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
    Networking.pools() ++
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
        {PartitionSupervisor, child_spec: Task.Supervisor, name: Logflare.TaskSupervisors},
        {PartitionSupervisor,
         child_spec: DynamicSupervisor, name: Logflare.Endpoints.ResultsCache.PartitionSupervisor},
        LogflareWeb.Endpoint,
        {Logflare.ActiveUserTracker,
         [name: Logflare.ActiveUserTracker, pubsub_server: Logflare.PubSub]}
      ]
  end

  defp get_children(_) do
    topologies = Application.get_env(:libcluster, :topologies, [])
    grpc_port = Application.get_env(:grpc, :port)
    ssl = Application.get_env(:logflare, :ssl)
    grpc_creds = if ssl, do: GRPC.Credential.new(ssl: ssl)
    pool_size = Application.get_env(:logflare, Logflare.PubSub)[:pool_size]

    # set goth early in the supervision tree
    Networking.pools() ++
      conditional_children() ++
      UserMonitoring.get_otel_exporter() ++
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
        Logs.RejectedLogEvents,
        # init Counters before Supervisof as Supervisor calls Counters through table create
        Counters,
        RateCounters,
        # Backends needs to be before Source.Supervisor
        Logflare.Backends,
        Logflare.Sources.Source.Supervisor,
        LogflareWeb.Endpoint,
        # If we get a log event and the Source.Supervisor is not up it will 500
        {GRPC.Server.Supervisor,
         endpoint: LogflareGrpc.Endpoint, port: grpc_port, cred: grpc_creds, start_server: true},
        # Monitor system level metrics
        SystemMetricsSup,
        Logflare.Telemetry,

        # For Logflare Endpoints
        {PartitionSupervisor,
         child_spec: DynamicSupervisor, name: Logflare.Endpoints.ResultsCache.PartitionSupervisor},

        # Startup tasks after v2 pipeline started
        {Task, fn -> startup_tasks() end},
        Logflare.Alerting.Supervisor,
        Logflare.Scheduler,
        # active users tracking for UserMetricsPoller
        {Logflare.ActiveUserTracker,
         [name: Logflare.ActiveUserTracker, pubsub_server: Logflare.PubSub]}
      ]
  end

  defp start_user_log_interceptor do
    if Application.get_env(:logflare, :env) == :test do
      :ok
    else
      :logger.add_primary_filter(
        :user_log_intercetor,
        {&UserMonitoring.log_interceptor/2, []}
      )
    end
  end

  def goth_partition_count, do: 5

  def conditional_children do
    goth =
      case BigQueryAdaptor.partitioned_goth_child_spec() do
        nil -> []
        goth_child_spec -> [goth_child_spec]
      end ++
        BigQueryAdaptor.impersonated_goth_child_specs()

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
