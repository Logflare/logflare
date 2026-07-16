defmodule Logflare.Backends.IngestEventQueue.GenerationJanitor do
  @moduledoc """
  Rotates and evicts generations for every `:pointer`-mode queue's shared event store.

  A single global sweep (mirroring `MapperJanitor`'s precedent), not one instance per
  backend: each tick, it discovers every `queues_key` that currently has a live
  generation (see `IngestEventQueue.list_generation_queues_keys/0` — the generations
  table itself is the source of truth, so this needs no knowledge of specific backends
  or adaptors), rotates in a fresh "current" generation for each, and drops any
  generation older than `max_age_ms` via a whole-table `:ets.delete/1` (see
  `IngestEventQueue.drop_generation/2`) — O(1) regardless of how much was left in it,
  unlike a per-row staleness scan.

  This is the bounded-loss cleanup mechanism for abandoned claims: a claim whose owner
  crashed before ack leaves its event unreferenced in the generation store (see
  `IngestEventQueue.pop_pending_pointers/2` — claiming deletes the pointer row outright,
  so there's nothing left to detect "abandoned"). Nothing here retries an abandoned
  claim; it just eventually disappears with its generation, bounded to roughly
  `max_age_ms` to `2 * max_age_ms` old.

  Each tick also sweeps the recent-events cache (see
  `IngestEventQueue.record_recent_event/2`, `sweep_recent_events/1`) down to
  `recent_events_max_age_ms` — a separate, much longer age bound than `max_age_ms`.
  That cache stores independent event copies now, not generation-store pointers (see
  its moduledoc section), so its retention is no longer coupled to generation
  eviction at all; it needs to instead comfortably outlast
  `Sources.source_idle?/1`'s 5-minute `has_recent_logs_within?/2` dependency, or a
  quiet source could get shut down before that window elapses.

  See `do_rotate/2` for a scoped, single-key variant used by tests that don't want the
  full global sweep's cost or blast radius.
  """
  use GenServer

  require Logger

  alias Logflare.Backends.IngestEventQueue

  @default_interval :timer.seconds(60)
  @default_max_age_ms :timer.minutes(2)
  @default_recent_events_max_age_ms :timer.minutes(10)

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl GenServer
  def init(opts) do
    state = %{
      interval: Keyword.get(opts, :interval, @default_interval),
      max_age_ms: Keyword.get(opts, :max_age_ms, @default_max_age_ms),
      recent_events_max_age_ms:
        Keyword.get(opts, :recent_events_max_age_ms, @default_recent_events_max_age_ms)
    }

    schedule(state)
    {:ok, state}
  end

  @impl GenServer
  def handle_info(:rotate, state) do
    do_rotate(state)
    schedule(state)
    {:noreply, state}
  end

  @doc false
  # Exposed for testing/benchmarking, same convention as QueueJanitor.do_drop/2.
  @spec do_rotate(map()) :: :ok
  def do_rotate(state) do
    do_rotate(state, IngestEventQueue.list_generation_queues_keys())
    IngestEventQueue.sweep_recent_events(state.recent_events_max_age_ms)
    :ok
  end

  @doc """
  Rotates and evicts only the given `queues_keys`, ignoring everything else
  currently live in the generation store — unlike `do_rotate/1`'s full global sweep.

  For production use this is never what you want (it's what `do_rotate/1` builds on
  top of); it exists so a test can drive rotation for a single, known key
  deterministically, without either of `do_rotate/1`'s two global side effects: cost
  that scales with however many *other* queues happen to be live process-wide at the
  time (the generation store is a shared, never-reset-between-tests singleton), and
  touching — and potentially evicting — generations that belong to other, unrelated
  tests' sources/backends.
  """
  @spec do_rotate(map(), [
          IngestEventQueue.queues_key()
          | IngestEventQueue.consolidated_queues_key()
          | IngestEventQueue.spool_producer_queues_key()
        ]) :: :ok
  def do_rotate(state, queues_keys) do
    # One call carrying every key, not one call per key — rotating an entire fleet of
    # queues shouldn't mean that many sequential round-trips to the same serialized
    # GenServer (see IngestEventQueue.new_generations/1).
    IngestEventQueue.new_generations(queues_keys)

    for queues_key <- queues_keys do
      drop_aged_generations(queues_key, state)
    end

    :ok
  end

  defp drop_aged_generations(queues_key, state) do
    cutoff = System.monotonic_time(:millisecond) - state.max_age_ms

    {dropped_count, dropped_size} =
      queues_key
      |> IngestEventQueue.list_generations()
      |> Enum.filter(fn {_tid, created_at} -> created_at <= cutoff end)
      |> Enum.reduce({0, 0}, fn {tid, _created_at}, {count, size} ->
        gen_size = table_size(tid)
        IngestEventQueue.drop_generation(queues_key, tid)
        {count + 1, size + gen_size}
      end)

    if dropped_count > 0 do
      emit_drop_telemetry(queues_key, dropped_count, dropped_size)
    end
  end

  defp table_size(tid) do
    case :ets.info(tid, :size) do
      n when is_integer(n) -> n
      _ -> 0
    end
  end

  defp emit_drop_telemetry(queues_key, dropped_count, dropped_size) do
    :telemetry.execute(
      [:logflare, :ingest_event_queue, :generation_janitor, :drop],
      %{generations: dropped_count, events: dropped_size},
      %{queues_key: queues_key}
    )
  end

  defp schedule(state) do
    Process.send_after(self(), :rotate, state.interval)
  end
end
