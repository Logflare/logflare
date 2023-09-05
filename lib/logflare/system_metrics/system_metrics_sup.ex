defmodule Logflare.SystemMetricsSup do
  @moduledoc false
  alias Logflare.SystemMetrics

  use Supervisor

  def start_link(args \\ []) do
    Supervisor.start_link(__MODULE__, args, name: __MODULE__)
  end

  @impl true
  def init(_init_arg) do
    children = [
      SystemMetrics.Observer.Poller,
      # This calls :erlang.process_info which may not be good to call for all proces frequently
      # SystemMetrics.Procs.Poller,
      SystemMetrics.AllLogsLogged,
      SystemMetrics.AllLogsLogged.Poller,
      SystemMetrics.Schedulers.Poller,
      SystemMetrics.Cachex.Poller
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
