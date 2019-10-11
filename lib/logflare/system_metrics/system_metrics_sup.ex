defmodule Logflare.SystemMetricsSup do
  alias Logflare.SystemMetrics

  use Supervisor

  def start_link() do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_init_arg) do
    children = [
      {SystemMetrics.Observer.Poller, []},
      {SystemMetrics.AllLogsLogged, []},
      {SystemMetrics.AllLogsLogged.Poller, []},
      {SystemMetrics.Schedulers.Poller, []}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
