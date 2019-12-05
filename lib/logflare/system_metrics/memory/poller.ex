defmodule Logflare.SystemMetrics.Memory.Poller do
  @moduledoc """
    Polls memory.
  """

  use GenServer

  alias Logflare.SystemMetrics.Memory

  require Logger

  @poll_every 30_000

  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args, name: __MODULE__)
  end

  def init(state) do
    poll_metrics()
    {:ok, state}
  end

  def handle_info(:poll_metrics, state) do
    observer_memory = Memory.get_memory()

    if Application.get_env(:logflare, :env) == :prod do
      Logger.info("Memory metrics!", observer_memory: observer_memory)
    end

    poll_metrics()
    {:noreply, state}
  end

  defp poll_metrics() do
    Process.send_after(self(), :poll_metrics, @poll_every)
  end
end
