defmodule Logflare.Application do
  @moduledoc false
  use Application

  alias Logflare.{
    Users,
    Sources,
    Logs,
    Billing,
    PubSubRates,
    ContextCache,
    SourceSchemas
  }

  def start(_type, _args) do
    env = Application.get_env(:logflare, :env)

    # Start distribution early for Cachex
    # can work with it.
    unless Node.alive?() or env in [:test] do
      {:ok, _} = Node.start(:logflare)
    end

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
      {Registry, name: Logflare.Backends.SourceRegistry, keys: :unique},
      {Registry, name: Logflare.Backends.SourceDispatcher, keys: :duplicate}
    ]
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

    tracker_pool_size = Application.get_env(:logflare, Logflare.Tracker)[:pool_size]
    topologies = Application.get_env(:libcluster, :topologies)

    [
      {Task.Supervisor, name: Logflare.TaskSupervisor},
      {Cluster.Supervisor, [topologies, [name: Logflare.ClusterSupervisor]]},
      get_goth_child_spec(),
      Logflare.Repo,
      {Phoenix.PubSub, name: Logflare.PubSub},
      {
        Logflare.Tracker,
        [
          name: Logflare.Tracker,
          pubsub_server: Logflare.PubSub,
          broadcast_period: 250,
          down_period: 5_000,
          permdown_period: 30_000,
          pool_size: tracker_pool_size,
          log_level: false
        ]
      },
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
      Sources.BuffersCache,
      Logs.RejectedLogEvents,
      # init Counters before Supervisof as Supervisor calls Counters through table create
      Sources.Counters,
      Sources.RateCounters,
      Logflare.PubSubRates,
      Logflare.Source.Supervisor,

      # If we get a log event and the Source.Supervisor is not up it will 500
      LogflareWeb.Endpoint,

      # Monitor system level metrics
      Logflare.SystemMetricsSup,

      # For Logflare Endpoints
      {DynamicSupervisor, strategy: :one_for_one, name: Logflare.Endpoints.Cache}
    ]
  end

  def config_change(changed, _new, removed) do
    LogflareWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
