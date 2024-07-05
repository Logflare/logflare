defmodule Logflare.Backends.IngestEventQueue.MapperJanitor do
  @moduledoc """
  Performs cleanup actions for a public-private :ets mapping table.

  This helps to clean up mappings where the table is dead, such as when the owner process dies and is not restarted due to new events.

  This is just for memory cleanup and to prevent memory leaks. If a tid is stale and dead, a new table will be created and upserted in IngestQueueEvent.
  """
  use GenServer
  alias Logflare.Backends.IngestEventQueue
  @default_interval 10_000

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def init(opts) do
    state = %{
      interval: Keyword.get(opts, :interval, @default_interval)
    }

    schedule(state.interval)
    {:ok, state}
  end

  def handle_info(:work, state) do
    IngestEventQueue.delete_stale_mappings()
    schedule(state.interval)
    {:noreply, state}
  end

  defp schedule(interval) do
    Process.send_after(self(), :work, interval)
  end
end
