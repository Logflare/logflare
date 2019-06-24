defmodule Logflare.SystemMetrics.Schedulers.Poller do
  use GenServer

  require Logger

  alias Logflare.SystemMetrics.Schedulers

  @poll_every 1_000

  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args, name: __MODULE__)
  end

  def init(_state) do
    poll_metrics()
    last_scheduler_metrics = :scheduler.sample()
    {:ok, last_scheduler_metrics}
  end

  def handle_info(:poll_metrics, last_scheduler_metrics) do
    poll_metrics()

    current_scheduler_metrics = :scheduler.sample()

    scheduler_metrics =
      Schedulers.scheduler_utilization(last_scheduler_metrics, current_scheduler_metrics)

    Logger.info("Scheduler metrics!", scheduler_metrics: scheduler_metrics)

    {:noreply, current_scheduler_metrics}
  end

  defp poll_metrics() do
    Process.send_after(self(), :poll_metrics, @poll_every)
  end
end
