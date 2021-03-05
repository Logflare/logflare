defmodule Logflare.Application do
  @moduledoc false
  use Application
  use Logflare.Commons

  def start(_type, _args) do
    import Supervisor.Spec

    # TODO: Set node status in GCP when sigterm is received
    :ok =
      :gen_event.swap_sup_handler(
        :erl_signal_server,
        {:erl_signal_handler, []},
        {Logflare.SigtermHandler, []}
      )

    tracker_pool_size = Application.get_env(:logflare, Logflare.Tracker)[:pool_size]

    LocalRepo.reset_mnesia()

    children = [
      PubSubRates.Cache,
      {Phoenix.PubSub, name: Logflare.PubSub},
      {
        Tracker,
        [
          [
            name: Tracker,
            pubsub_server: Logflare.PubSub,
            broadcast_period: 1_000,
            down_period: 5_000,
            permdown_period: 30_000,
            pool_size: tracker_pool_size,
            log_level: false
          ]
        ]
      },
      Repo,
      LocalRepo,
      LocalRepo.Migrations,
      # LocalRepo.Sync,
      {Changefeeds.ListenerSupervisor, changefeeds: Changefeeds.list_changefeed_channels()},
      LogflareWeb.Endpoint,
      {Task.Supervisor, name: Logflare.TaskSupervisor}
    ]

    topologies = Application.get_env(:libcluster, :topologies)

    dev_prod_children = [
      {Task.Supervisor, name: Logflare.TaskSupervisor},
      {Cluster.Supervisor, [topologies, [name: Logflare.ClusterSupervisor]]},
      Repo,
      LocalRepo.Migrations,
      LocalRepo,
      LocalRepo.Sync,
      {Changefeeds.ListenerSupervisor, changefeeds: Changefeeds.list_changefeed_channels()},
      {Repo.MaxPartitionedRowsWorker,
       [
         %{
           schema: RejectedLogEvent,
           opts: %{partition_by: :source_id, order_by: [desc: :ingested_at], limit: 500}
         }
       ]},
      {Changefeeds.RepoStateSyncValidator, [interval_sec: 300]},
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
      PubSubRates.Cache,
      Sources.Buffers,
      Sources.BuffersCache,
      # init Counters before Manager as Manager calls Counters through table create
      {Sources.Counters, []},
      {Sources.RateCounters, []},
      {Logflare.PubSubRates, []},
      {Logflare.Source.Supervisor, []},
      {Logflare.SystemMetricsSup, []},
      {LogflareWeb.Endpoint, []}
    ]

    env = Application.get_env(:logflare, :env)

    children =
      if env == :test do
        children
      else
        dev_prod_children
      end

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
