defmodule Logflare.Application do
  @moduledoc false
  use Application

  alias Logflare.{
    Users,
    Sources,
    Tracker,
    Logs,
    BillingAccounts,
    Plans,
    PubSubRates,
    ContextCache,
    SourceSchemas
  }

  def start(_type, _args) do
    # Database options for Postgres notifications
    hostname = '#{Application.get_env(:logflare, Logflare.Repo)[:hostname]}'
    username = Application.get_env(:logflare, Logflare.Repo)[:username]
    password = Application.get_env(:logflare, Logflare.Repo)[:password]
    database = Application.get_env(:logflare, Logflare.Repo)[:database]
    port = Application.get_env(:logflare, Logflare.Repo)[:port]
    slot = Application.get_env(:logflare, Logflare.CacheBuster)[:replication_slot]
    publications = Application.get_env(:logflare, Logflare.CacheBuster)[:publications]

    env = Application.get_env(:logflare, :env)

    # Start distribution early so that both Cachex and Logflare.SQL
    # can work with it.
    unless Node.alive?() do
      {:ok, _} = Node.start(:logflare)
    end

    # Setup Goth for GCP connections
    credentials =
      if env in [:dev, :test],
        do: Application.get_env(:goth, :json) |> Jason.decode!(),
        else: System.get_env("GOOGLE_APPLICATION_CREDENTIALS") |> File.read!() |> Jason.decode!()

    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    source = {:service_account, credentials, scopes: scopes}

    # TODO: Set node status in GCP when sigterm is received
    :ok =
      :gen_event.swap_sup_handler(
        :erl_signal_server,
        {:erl_signal_handler, []},
        {Logflare.SigtermHandler, []}
      )

    tracker_pool_size = Application.get_env(:logflare, Logflare.Tracker)[:pool_size]

    children = [
      ContextCache,
      Users.Cache,
      Sources.Cache,
      BillingAccounts.Cache,
      Plans.Cache,
      SourceSchemas.Cache,
      PubSubRates.Cache,
      Logs.LogEvents.Cache,
      Logs.RejectedLogEvents,
      {Phoenix.PubSub, name: Logflare.PubSub},
      Logflare.Repo,
      {Goth, name: Logflare.Goth, source: source},
      LogflareWeb.Endpoint,
      {Task.Supervisor, name: Logflare.TaskSupervisor}
    ]

    topologies = Application.get_env(:libcluster, :topologies)

    dev_prod_children = [
      {Task.Supervisor, name: Logflare.TaskSupervisor},
      {Cluster.Supervisor, [topologies, [name: Logflare.ClusterSupervisor]]},
      {Goth, name: Logflare.Goth, source: source},
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
      BillingAccounts.Cache,
      Plans.Cache,
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
      Sources.Buffers,
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
      Logflare.SQL,
      {DynamicSupervisor, strategy: :one_for_one, name: Logflare.Endpoint.Cache}
    ]

    children = if env in [:test], do: children, else: dev_prod_children

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Logflare.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def config_change(changed, _new, removed) do
    LogflareWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
