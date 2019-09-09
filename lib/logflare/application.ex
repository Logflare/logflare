defmodule Logflare.Application do
  @moduledoc false
  use Application

  def start(_type, _args) do
    import Supervisor.Spec

    children = [
      Logflare.Users.Cache,
      Logflare.Sources.Cache,
      Logflare.Logs.RejectedLogEvents,
      supervisor(Logflare.Repo, []),
      supervisor(LogflareWeb.Endpoint, []),
      {Task.Supervisor, name: Logflare.TaskSupervisor}
    ]

    topologies = Application.get_env(:libcluster, :topologies)

    dev_prod_children = [
      {Cluster.Supervisor, [topologies, [name: Logflare.ClusterSupervisor]]},
      supervisor(Logflare.Repo, []),
      Logflare.Users.Cache,
      Logflare.Sources.Cache,
      Logflare.Logs.RejectedLogEvents,
      # init Counters before Manager as Manager calls Counters through table create
      {Task.Supervisor, name: Logflare.TaskSupervisor},
      supervisor(Logflare.Sources.Counters, []),
      supervisor(Logflare.Sources.RateCounters, []),
      supervisor(Logflare.SystemMetrics, []),
      supervisor(Logflare.Source.Supervisor, []),
      supervisor(LogflareWeb.Endpoint, []),
      {Logflare.Tracker,
       [name: Logflare.Tracker, pubsub_server: Logflare.PubSub, broadcast_period: 250]}
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
