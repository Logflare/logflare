defmodule Logflare.Backends.IngestEventQueue.MapperJanitor do
  @moduledoc """
  Performs cleanup actions for a public-private :ets mapping table
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
