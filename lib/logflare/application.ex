defmodule Logflare.Application do
  @moduledoc false
  use Application

  def start(_type, _args) do
    import Supervisor.Spec

    :ok =
      :gen_event.swap_sup_handler(
        :erl_signal_server,
        {:erl_signal_handler, []},
        {Logflare.SigtermHandler, []}
      )

    children = [
      Logflare.Users.Cache,
      Logflare.Sources.Cache,
      Logflare.Logs.RejectedLogEvents,
      supervisor(Logflare.Repo, []),
      supervisor(LogflareWeb.Endpoint, []),
      {Task.Supervisor, name: Logflare.TaskSupervisor}
    ]

    topologies = Application.get_env(:libcluster, :topologies)
    tracker_pool_size = Application.get_env(:logflare, Logflare.Tracker)[:pool_size]

    dev_prod_children = [
      {Cluster.Supervisor, [topologies, [name: Logflare.ClusterSupervisor]]},
      supervisor(Logflare.Repo, []),
      supervisor(Phoenix.PubSub.PG2, [
        [
          name: Logflare.PubSub,
          fastlane: Phoenix.Channel.Server
        ]
      ]),
      worker(
        Logflare.Tracker,
        [
          [
            name: Logflare.Tracker,
            pubsub_server: Logflare.PubSub,
            broadcast_period: 1_000,
            down_period: 5_000,
            permdown_period: 30_000,
            pool_size: tracker_pool_size,
            log_level: false
          ]
        ]
      ),
      # supervisor(LogflareTelemetry.Supervisor, []),
      Logflare.Users.Cache,
      Logflare.Sources.Cache,
      Logflare.Tracker.Cache,
      Logflare.Sources.Buffers,
      Logflare.Logs.RejectedLogEvents,
      # init Counters before Manager as Manager calls Counters through table create
      {Task.Supervisor, name: Logflare.TaskSupervisor},
      supervisor(Logflare.Sources.Counters, []),
      supervisor(Logflare.Sources.RateCounters, []),
      supervisor(Logflare.Tracker.SourceNodeInserts, []),
      supervisor(Logflare.Tracker.SourceNodeBuffers, []),
      supervisor(Logflare.Tracker.SourceNodeRates, []),
      supervisor(Logflare.Source.Supervisor, []),
      supervisor(Logflare.SystemMetricsSup, []),
      supervisor(LogflareWeb.Endpoint, [])
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
