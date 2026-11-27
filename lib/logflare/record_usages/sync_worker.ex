defmodule Logflare.RecordUsages.SyncWorker do
  @moduledoc """
  Flushes the node's usage on a jittered timer. A failed flush restarts the worker and retains unacknowledged usage.
  """

  use GenServer

  alias Logflare.RecordUsages
  alias Logflare.Utils

  @sync_interval_ms 300_000

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, Keyword.get(opts, :interval, @sync_interval_ms),
      name: __MODULE__
    )
  end

  @impl GenServer
  @spec init(pos_integer()) :: {:ok, pos_integer()}
  def init(interval) do
    Process.send_after(self(), :flush, Enum.random(1..interval))
    {:ok, interval}
  end

  @impl GenServer
  @spec handle_info(:flush, pos_integer()) :: {:noreply, pos_integer()}
  def handle_info(:flush, interval) do
    RecordUsages.flush(drain: false)
    schedule_flush(interval)
    {:noreply, interval}
  end

  @spec schedule_flush(pos_integer()) :: reference()
  defp schedule_flush(interval) do
    Process.send_after(self(), :flush, Utils.jitter(interval, 0.1))
  end
end
