defmodule Logflare.Backends.Spool.EventQueue do
  @moduledoc """
  ETS-backed FIFO queue of ingest chunks for the spool producer.

  Each push is one caller's whole batch of events, stored and claimed as a
  single atomic unit — a chunk is never split across two commits, so its
  fate (durably uploaded + queue-notified, or failed) always maps back to
  exactly one caller to reply to.

  Storage mirrors `Logflare.Backends.IngestEventQueue`'s pointer/generation-store
  split, for the same reason: `pop/1` only ever hands out a lightweight `Chunk`
  pointer (see its moduledoc), never the event bodies, so nothing gets copied
  through Broadway's stage hops until `get_events/1` resolves it exactly once, in
  `Spool.ProducerPipeline.handle_batch/4`. Unlike `IngestEventQueue`, spool doesn't
  need per-event dedup, round-robin distribution across multiple producer
  instances, or fine-grained per-event claiming — a chunk is claimed and acked as a
  whole, and there is exactly one spool producer pipeline per node, so two plain
  global tables (no generation rotation) are enough.
  """

  alias Logflare.Backends.Spool.EventQueue.Chunk
  alias Logflare.LogEvent

  @table __MODULE__
  @bodies __MODULE__.Bodies
  @default_ack_timeout 15_000

  @spec init_table() :: :ok
  def init_table do
    if :ets.whereis(@table) == :undefined do
      :ets.new(@table, [
        :named_table,
        :public,
        :ordered_set,
        write_concurrency: true,
        read_concurrency: true
      ])
    end

    if :ets.whereis(@bodies) == :undefined do
      :ets.new(@bodies, [
        :named_table,
        :public,
        :set,
        write_concurrency: true,
        read_concurrency: true
      ])
    end

    :ok
  end

  @doc """
  Enqueues one chunk of events, fire-and-forget — returns immediately, before
  the chunk's fate (durable or failed) is known. Use `push_sync/2` to block
  until that's known instead.

  `caller_pid`, if given, is who `Spool.ProducerPipeline.ack/3` notifies once
  the chunk's fate is known — `nil` (the default) if nobody's waiting. A
  no-op for an empty event list.
  """
  @spec push([LogEvent.t()], pid() | nil) :: {:ok, reference()} | :ok
  def push(events, caller_pid \\ nil)
  def push([], _caller_pid), do: :ok
  def push(events, caller_pid), do: enqueue(events, caller_pid)

  @doc """
  Enqueues one chunk of events and blocks the caller until
  `Spool.ProducerPipeline.ack/3` replies with the chunk's fate, or `timeout`
  elapses — see its moduledoc for why a chunk's fate is always a single,
  atomic `:ok`/`{:error, reason}`. A no-op (returns `:ok` immediately) for an
  empty event list.

  `timeout` is a fixed default, not something ingest call sites configure —
  it's an internal detail of how long we wait for a durability round trip,
  not a per-request policy.
  """
  @spec push_sync([LogEvent.t()], timeout()) :: :ok | {:error, term()}
  def push_sync(events, timeout \\ @default_ack_timeout)
  def push_sync([], _timeout), do: :ok

  def push_sync(events, timeout) do
    {:ok, ref} = enqueue(events, self())

    result =
      receive do
        {^ref, result} -> result
      after
        timeout -> {:error, :timeout}
      end

    result
  end

  defp enqueue(events, caller_pid) do
    ref = make_ref()

    :ets.insert(@bodies, {ref, events})

    pointer = %Chunk{
      ref: ref,
      caller_pid: caller_pid,
      byte_size: chunk_byte_size(events),
      event_count: length(events),
      retries: 0
    }

    :ets.insert(@table, {:erlang.unique_integer([:monotonic]), pointer})
    {:ok, ref}
  end

  @doc """
  Resolves a chunk pointer's event bodies — a single `:ets.lookup_element/4`,
  same as `IngestEventQueue.lookup_event/2`. Called exactly once per chunk, by
  `Spool.ProducerPipeline.handle_batch/4` right before encoding. Returns `[]` if
  the chunk was already finalized (see `delete_events/1`) — shouldn't happen in
  practice, since a chunk's body is only deleted after its pointer's terminal
  ack, but treated as a miss rather than a crash for the same reason
  `IngestEventQueue.lookup_event/2` is.
  """
  @spec get_events(reference()) :: [LogEvent.t()]
  def get_events(ref) do
    :ets.lookup_element(@bodies, ref, 2, [])
  end

  @doc """
  Deletes a chunk's event bodies once its pointer has reached a terminal state
  (acked successfully, or exhausted its retries) — see
  `Spool.ProducerPipeline.reply/2`, the only caller. Must not be called for a
  chunk that's only being requeued for retry (see `requeue/1`); the body has to
  stay put for the next attempt's `get_events/1`.
  """
  @spec delete_events(reference()) :: :ok
  def delete_events(ref) do
    :ets.delete(@bodies, ref)
    :ok
  end

  @doc """
  Pops up to `max_chunks` chunks, oldest first. Returns fewer than
  `max_chunks` (including none) if the queue doesn't have that many.
  """
  @spec pop(non_neg_integer()) :: [Chunk.t()]
  def pop(max_chunks) when is_integer(max_chunks) and max_chunks <= 0, do: []

  def pop(max_chunks) when is_integer(max_chunks) do
    case :ets.select(@table, [{{:"$1", :_}, [], [:"$1"]}], max_chunks) do
      {keys, _cont} -> take(keys)
      :"$end_of_table" -> []
    end
  end

  defp take(keys) do
    Enum.flat_map(keys, fn key ->
      case :ets.take(@table, key) do
        [{^key, chunk}] -> [chunk]
        [] -> []
      end
    end)
  end

  @doc """
  Re-enqueues a chunk for retry: bumps its retry count and moves it to the
  back of the queue. Keeps the same `ref`, so the original caller (if any)
  is still resolvable once the retry lands.
  """
  @spec requeue(Chunk.t()) :: :ok
  def requeue(%Chunk{} = chunk) do
    bumped = %{chunk | retries: chunk.retries + 1}
    :ets.insert(@table, {:erlang.unique_integer([:monotonic]), bumped})
    :ok
  end

  @doc "Number of chunks currently queued — diagnostic/telemetry use."
  @spec count() :: non_neg_integer()
  def count, do: :ets.info(@table, :size)

  defp chunk_byte_size(events) do
    Enum.reduce(events, 0, fn event, acc -> acc + :erlang.external_size(event.body) end)
  end
end
