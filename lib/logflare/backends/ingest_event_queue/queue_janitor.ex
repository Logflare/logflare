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
  @default_max Logflare.Backends.max_buffer_len()
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

    schedule(state, false)
    {:ok, state}
  end

  def handle_info(:work, state) do
    do_drop(state)
    scale? = if Application.get_env(:logflare, :env) == :test, do: false, else: true

    schedule(state, scale?)

    {:noreply, state}
  end

  # expose for benchmarking
  def do_drop(state) do
    sid_bid = {state.source_id, state.backend_id}
    # safety measure, drop all if still exceed
    for sid_bid_pid <- IngestEventQueue.list_queues(sid_bid),
        pending_size = IngestEventQueue.count_pending(sid_bid_pid),
        is_integer(pending_size) do
      if pending_size > state.remainder do
        IngestEventQueue.truncate_table(sid_bid_pid, :ingested, 0)
      else
        IngestEventQueue.truncate_table(sid_bid_pid, :ingested, state.remainder)
      end

      if pending_size > state.max do
        to_drop = round(state.purge_ratio * pending_size)
        IngestEventQueue.drop(sid_bid_pid, :pending, to_drop)

        Logger.warning(
          "IngestEventQueue private :ets buffer exceeded max for source id=#{state.source_id}, dropping #{to_drop} events",
          backend_id: state.backend_id
        )
      end
    end
  end

  # schedule work based on rps
  defp schedule(state, scale?) do
    metrics = Sources.get_source_metrics_for_ingest(state.source_token)
    # dynamically schedule based on metrics interval
    interval =
      cond do
        scale? == false ->
          state.interval

        metrics.avg < 100 ->
          state.interval * 10

        metrics.avg < 1000 ->
          state.interval * 5

        metrics.avg < 2000 ->
          state.interval * 2.5

        true ->
          state.interval
      end
      |> round()

    Process.send_after(self(), :work, interval)
  end
end
