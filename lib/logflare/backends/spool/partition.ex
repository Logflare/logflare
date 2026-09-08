defmodule Logflare.Backends.Spool.Partition do
  @moduledoc """
  Accumulates pre-compressed, pre-framed segments pushed directly by
  ingest callers via `append/5`/`append_async/4` — no ETS, no poll
  interval (compare `Logflare.Backends.Spool.ChunkProducer`, which this
  replaces). Hands off to its paired `Committer` once a byte/event budget
  is hit or `batch_timeout` elapses, matching
  `ProducerPipeline.spool_batch_size_splitter/0`'s old thresholds.

  Pipelined like `durable_buffer`'s Partition/Committer: the moment a
  commit is handed off, new appends keep accumulating into the *next*
  batch — see `handle_info/2`'s `:commit_done` clause, which flushes
  immediately if anything piled up while the previous commit was in
  flight, rather than waiting for a fresh timeout.
  """

  use GenServer

  alias Logflare.Backends.Spool.Committer
  alias Logflare.Backends.Spool.MemoryMonitor

  @max_batch_bytes 7 * 1024 * 1024
  @early_flush_batch_bytes 3 * 1024 * 1024
  @max_batch_count 500_000

  @type entry ::
          {GenServer.from() | nil, binary(), non_neg_integer(), non_neg_integer(),
           non_neg_integer()}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))
  end

  @doc "Blocks the caller until the chunk this appends is durable or fails."
  @spec append(GenServer.server(), binary(), non_neg_integer(), non_neg_integer(), timeout()) ::
          :ok | {:error, term()}
  def append(partition, segment, byte_size, event_count, timeout \\ 15_000) do
    GenServer.call(partition, {:append, segment, byte_size, event_count}, timeout)
  end

  @doc "Enqueues a segment fire-and-forget."
  @spec append_async(GenServer.server(), binary(), non_neg_integer(), non_neg_integer()) :: :ok
  def append_async(partition, segment, byte_size, event_count) do
    GenServer.cast(partition, {:append_async, segment, byte_size, event_count})
  end

  @spec requeue(GenServer.server(), [entry()]) :: :ok
  def requeue(partition, entries), do: GenServer.cast(partition, {:requeue, entries})

  @impl GenServer
  def init(opts) do
    committer_opts =
      Keyword.take(opts, [
        :index,
        :bucket,
        :storage_mod,
        :queue_mod,
        :queue_ref,
        :format,
        :compress,
        :compression_algorithm
      ])

    {:ok, committer} = Committer.start_link([{:partition, self()} | committer_opts])

    {:ok,
     %{
       committer: committer,
       batch_timeout: Keyword.fetch!(opts, :batch_timeout),
       pending: [],
       pending_bytes: 0,
       pending_count: 0,
       flush_scheduled?: false,
       timer_ref: nil,
       in_flight: 0
     }}
  end

  @impl GenServer
  def handle_call({:append, segment, byte_size, event_count}, from, state) do
    state
    |> put_pending({from, segment, byte_size, event_count, 0})
    |> maybe_flush(:size)
    |> then(&{:noreply, &1})
  end

  @impl GenServer
  def handle_cast({:append_async, segment, byte_size, event_count}, state) do
    state
    |> put_pending({nil, segment, byte_size, event_count, 0})
    |> maybe_flush(:size)
    |> then(&{:noreply, &1})
  end

  def handle_cast({:requeue, entries}, state) do
    state
    |> then(fn s -> Enum.reduce(entries, s, &put_pending(&2, &1)) end)
    |> maybe_flush(:size)
    |> then(&{:noreply, &1})
  end

  @impl GenServer
  def handle_info(:flush, state) do
    state = %{state | flush_scheduled?: false, timer_ref: nil}

    state =
      if state.pending != [] and state.in_flight == 0, do: handoff(state, :timeout), else: state

    {:noreply, state}
  end

  def handle_info(:commit_done, state) do
    state = %{state | in_flight: state.in_flight - 1}

    state =
      if state.pending != [] and state.in_flight == 0, do: handoff(state, :pipeline), else: state

    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp put_pending(state, {_from, _segment, byte_size, event_count, _retries} = entry) do
    %{
      state
      | pending: [entry | state.pending],
        pending_bytes: state.pending_bytes + byte_size,
        pending_count: state.pending_count + event_count
    }
  end

  defp maybe_flush(state, trigger) do
    cond do
      state.pending_bytes >= max_batch_bytes() or state.pending_count >= @max_batch_count ->
        handoff(state, trigger)

      state.flush_scheduled? or state.in_flight > 0 ->
        state

      true ->
        ref = Process.send_after(self(), :flush, state.batch_timeout)
        %{state | flush_scheduled?: true, timer_ref: ref}
    end
  end

  defp max_batch_bytes do
    if MemoryMonitor.throttled?(), do: @early_flush_batch_bytes, else: @max_batch_bytes
  end

  defp handoff(state, trigger) do
    Committer.commit(
      state.committer,
      state.pending,
      state.pending_bytes,
      state.pending_count,
      trigger
    )

    if state.timer_ref, do: Process.cancel_timer(state.timer_ref)

    %{
      state
      | pending: [],
        pending_bytes: 0,
        pending_count: 0,
        flush_scheduled?: false,
        timer_ref: nil,
        in_flight: state.in_flight + 1
    }
  end
end
