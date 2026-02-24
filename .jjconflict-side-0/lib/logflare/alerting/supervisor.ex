defmodule Logflare.Alerting.Supervisor do
  @moduledoc false
  use Supervisor

  alias Logflare.Alerting.AlertsScheduler
  alias Logflare.Alerting
  alias Logflare.GenSingleton

  def start_link(args) do
    Supervisor.start_link(__MODULE__, args, name: __MODULE__)
  end

  @impl Supervisor
  def init(_args) do
    Supervisor.init(
      [
        {GenSingleton, child_spec: {AlertsScheduler, name: Alerting.scheduler_name()}}
      ],
      strategy: :one_for_one
    )
  end
end
