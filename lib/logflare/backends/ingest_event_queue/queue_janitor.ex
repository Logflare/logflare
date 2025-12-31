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
  @default_max Logflare.Backends.max_buffer_queue_len()
  @default_purge_ratio 0.05

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

    handle_info(:work, state)
    {:ok, state}
  end

  def handle_info(:work, state) do
    scale? = if Application.get_env(:logflare, :env) == :test, do: false, else: true
    metrics = Sources.get_source_metrics_for_ingest(state.source_token)
    do_drop(state, metrics)

    schedule(state, scale?, metrics)

    {:noreply, state}
  end

  # expose for benchmarking
  def do_drop(state, metrics) do
    sid_bid = {state.source_id, state.backend_id}
    # safety measure, drop all if still exceed
    for {{_sid, bid, pid} = sid_bid_pid, size} <- IngestEventQueue.list_counts(sid_bid) do
      if metrics.avg > 100 or bid != nil do
        IngestEventQueue.truncate_table(sid_bid_pid, :ingested, 0)
      else
        IngestEventQueue.truncate_table(sid_bid_pid, :ingested, state.remainder)
      end

      size = IngestEventQueue.get_table_size(sid_bid_pid)

      if size > state.max and pid != nil and is_integer(size) do
        to_drop = round(state.purge_ratio * size)
        IngestEventQueue.drop(sid_bid_pid, :pending, to_drop)

        Logger.warning(
          "IngestEventQueue private :ets buffer exceeded max for source id=#{state.source_id}, dropping #{to_drop} events",
          backend_id: state.backend_id,
          source_id: state.source_token,
          source_token: state.source_token,
          ingest_drop_count: to_drop
        )
      end
    end
  end

  # schedule work based on rps
  defp schedule(state, scale?, metrics) do
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
