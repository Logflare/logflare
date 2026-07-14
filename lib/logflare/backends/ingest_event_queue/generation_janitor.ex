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
  `IngestEventQueue.take_pending_pointers/2` — claiming deletes the pointer row outright,
  so there's nothing left to detect "abandoned"). Nothing here retries an abandoned
  claim; it just eventually disappears with its generation, bounded to roughly
  `max_age_ms` to `2 * max_age_ms` old.

  Each tick also sweeps the recent-events cache (see
  `IngestEventQueue.record_recent_event/2`, `sweep_recent_events/1`) down to the same
  `max_age_ms`.
  """
  use GenServer

  require Logger

  alias Logflare.Backends.IngestEventQueue

  @default_interval :timer.seconds(60)
  @default_max_age_ms :timer.minutes(2)

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl GenServer
  def init(opts) do
    state = %{
      interval: Keyword.get(opts, :interval, @default_interval),
      max_age_ms: Keyword.get(opts, :max_age_ms, @default_max_age_ms)
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
    for queues_key <- IngestEventQueue.list_generation_queues_keys() do
      rotate_queue(queues_key, state)
    end

    IngestEventQueue.sweep_recent_events(state.max_age_ms)

    :ok
  end

  defp rotate_queue(queues_key, state) do
    IngestEventQueue.new_generation(queues_key)

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

    Logger.warning(
      "GenerationJanitor: dropped #{dropped_count} generation(s) (#{dropped_size} event(s)) for #{inspect(queues_key)}",
      queues_key: inspect(queues_key)
    )
  end

  defp schedule(state) do
    Process.send_after(self(), :rotate, state.interval)
  end
end
