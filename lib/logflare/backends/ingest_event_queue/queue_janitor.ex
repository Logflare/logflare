defmodule Logflare.Backends.IngestEventQueue.QueueJanitor do
  @moduledoc """
  Performs cleanup actions for a private :ets queue

  Periodically purges the queue of `:ingested` events.

  If total events exceeds a max threshold, it will purge all events from the queue.
  This is in the case of sudden bursts of events that do not get cleared fast enough.
  It also acts as a failsafe for any potential runaway queue buildup from bugs.
  """
  use GenServer
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Sources
  require Logger
  @default_interval 1_000
  @default_remainder 100
  @default_max 50_000
  @default_purge_ratio 0.1

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def init(opts) do
    bid = if backend = Keyword.get(opts, :backend), do: backend.id
    source = Keyword.get(opts, :source)

    state = %{
      source_id: source.id,
      source_token: source.token,
      backend_id: bid,
      interval: Keyword.get(opts, :interval, @default_interval),
      remainder: Keyword.get(opts, :remainder, @default_remainder),
      max: Keyword.get(opts, :max, @default_max),
      purge_ratio: Keyword.get(opts, :purge_ratio, @default_purge_ratio)
    }

    schedule(state.interval)
    {:ok, state}
  end

  def handle_info(:work, state) do
    do_drop(state)

    metrics = Sources.get_source_metrics_for_ingest(state.source_token)
    # dynamically schedule based on metrics interval
    cond do
      metrics.avg < 100 ->
        schedule(state.interval * 10)

      metrics.avg < 1000 ->
        schedule(state.interval * 5)

      metrics.avg < 2000 ->
        schedule(state.interval * 2.5)

      true ->
        schedule(state.interval)
    end

    {:noreply, state}
  end

  # expose for benchmarking
  def do_drop(state) do
    sid_bid = {state.source_id, state.backend_id}
    # clear out ingested events
    pending_size = IngestEventQueue.count_pending(sid_bid)

    if pending_size != nil and pending_size > state.remainder do
      # drop all ingested
      IngestEventQueue.truncate(sid_bid, :ingested, 0)
    else
      IngestEventQueue.truncate(sid_bid, :ingested, state.remainder)
    end

    # safety measure, drop all if still exceed
    if pending_size != nil and pending_size > state.max do
      to_drop = round(state.purge_ratio * pending_size)
      IngestEventQueue.drop(sid_bid, :all, to_drop)

      Logger.warning(
        "IngestEventQueue private :ets buffer exceeded max for source id=#{state.source_id}, dropping #{pending_size} events",
        backend_id: state.backend_id
      )
    end
  end

  # schedule work based on rps
  defp schedule(interval) do
    Process.send_after(self(), :work, interval)
  end
end
