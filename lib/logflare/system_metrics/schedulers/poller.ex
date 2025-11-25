defmodule Logflare.SystemMetrics.Schedulers.Poller do
  @moduledoc false
  use GenServer

  require Logger

  alias Logflare.SystemMetrics.Schedulers

  @poll_every 5_000

  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args, name: __MODULE__)
  end

  def init(_state) do
    poll_metrics(Enum.random(0..:timer.seconds(60)))
    last_scheduler_metrics = :scheduler.sample()
    {:ok, last_scheduler_metrics}
  end

  def handle_info(:poll_metrics, last_scheduler_metrics) do
    current_scheduler_metrics = :scheduler.sample()

    scheduler_metrics =
      Schedulers.scheduler_utilization(last_scheduler_metrics, current_scheduler_metrics)

    if Application.get_env(:logflare, :env) == :prod do
      Logger.info("Scheduler metrics!", scheduler_metrics: scheduler_metrics)
    end

    poll_metrics()
    {:noreply, current_scheduler_metrics}
  end

  defp poll_metrics(every \\ @poll_every) do
    Process.send_after(self(), :poll_metrics, every)
  end
end
