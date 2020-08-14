defmodule Logflare.SystemMetrics.Observer.Poller do
  @moduledoc """
    Polls some observer stats.
  """

  use GenServer

  alias Logflare.SystemMetrics.Observer

  require Logger

  @poll_every 30_000

  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args, name: __MODULE__)
  end

  def init(state) do
    poll_metrics(Enum.random(0..:timer.seconds(60)))
    {:ok, state}
  end

  def handle_info(:poll_metrics, state) do
    observer_metrics = Observer.get_metrics()

    if Application.get_env(:logflare, :env) == :prod do
      Logger.info("Observer metrics!", observer_metrics: observer_metrics)
    end

    poll_metrics()
    {:noreply, state}
  end

  defp poll_metrics(every \\ @poll_every) do
    Process.send_after(self(), :poll_metrics, every)
  end
end
