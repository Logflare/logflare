defmodule Logflare.SystemMetricsSup do
  @moduledoc false
  alias Logflare.SystemMetrics
  alias Logflare.SystemMetrics.Observer
  alias Logflare.SystemMetrics.Cluster

  use Supervisor

  def start_link(args \\ []) do
    Supervisor.start_link(__MODULE__, args, name: __MODULE__)
  end

  @impl true
  def init(_init_arg) do
    children = [
      SystemMetrics.AllLogsLogged,
      SystemMetrics.AllLogsLogged.Poller,
      # telemetry poller
      {
        :telemetry_poller,
        measurements: [
          {Observer, :dispatch_stats, []},
          {Cluster, :dispatch_stats, []},
          {SystemMetrics.Schedulers, :async_dispatch_stats, []},
          {Logflare.SystemMetrics.Cluster, :finch, []}
        ],
        period: :timer.seconds(30),
        init_delay: :timer.seconds(30),
        name: Logflare.TelemetryPoller.Perodic
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
