defmodule Logflare.Backends.IngestEventQueue.QueueJanitor do
  @moduledoc """
  Performs cleanup actions for a private :ets queue
  """
  use GenServer
  alias Logflare.Backends.IngestEventQueue
  require Logger
  @default_interval 1_000
  @default_remainder 100
  @default_max 50_000

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def init(opts) do
    bid = if backend = Keyword.get(opts, :backend), do: backend.id
    source = Keyword.get(opts, :source)

    state = %{
      source_id: source.id,
      backend_id: bid,
      interval: Keyword.get(opts, :interval, @default_interval),
      remainder: Keyword.get(opts, :remainder, @default_remainder),
      max: Keyword.get(opts, :max, @default_max)
    }

    schedule(state.interval)
    {:ok, state}
  end

  def handle_info(:work, state) do
    sid_bid = {state.source_id, state.backend_id}
    size = IngestEventQueue.get_table_size(sid_bid)

    if size > state.max do
      IngestEventQueue.truncate(sid_bid, :all, 0)

      Logger.warning(
        "IngestEventQueue private :ets buffer exceeded max for source #{state.source_id}, dropping #{size} events",
        backend_id: state.backend_id
      )
    else
      IngestEventQueue.truncate(sid_bid, :ingested, state.remainder)
    end

    schedule(state.interval)
    {:noreply, state}
  end

  defp schedule(interval) do
    Process.send_after(self(), :work, interval)
  end
end
