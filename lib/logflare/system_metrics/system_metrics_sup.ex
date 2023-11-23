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
      # This calls :erlang.process_info which may not be good to call for all proces frequently
      # SystemMetrics.Procs.Poller,
      SystemMetrics.AllLogsLogged,
      SystemMetrics.AllLogsLogged.Poller,
      SystemMetrics.Schedulers.Poller,
      SystemMetrics.Cachex.Poller,
      # telemetry poller
      {
        :telemetry_poller,
        # include custom measurement as an MFA tuple
        # configure sampling period - default is :timer.seconds(5)
        # configure sampling initial delay - default is 0
        measurements: [
          {Observer, :dispatch_stats, []},
          {Cluster, :dispatch_stats, []}
        ],
        period: :timer.seconds(30),
        init_delay: :timer.seconds(30),
        name: Logflare.TelemetryPoller.Perodic
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
