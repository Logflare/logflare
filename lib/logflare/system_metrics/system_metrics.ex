defmodule Logflare.SystemMetrics do
  alias Logflare.SystemMetrics

  use Supervisor

  def start_link() do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_init_arg) do
    children = [
      {SystemMetrics.Observer.PerSecond, []},
      {SystemMetrics.AllLogsLogged, []},
      {SystemMetrics.AllLogsLogged.PerSecond, []}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
