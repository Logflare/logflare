defmodule Logflare.Backends.Spool.Partition do
  @moduledoc """
  Accumulates pre-compressed, pre-framed segments pushed directly by ingest
  callers via `append/5`/`append_async/4` — no ETS, no poll interval (compare
  `Logflare.Backends.Spool.ChunkProducer`, which this replaces).

  Every append is written and `datasync`ed to a local WAL file *before*
  replying — durability is local-disk-durable, not GCS/Pub-Sub-durable, which
  is what makes this fast: a local `fsync` instead of a network round trip.

  Segments accumulate in the *active* WAL file under one continuous flush
  loop: a `batch_timeout` timer (config, default 1s) is the sole steady-state
  trigger, rolling whatever has accumulated so far, however little — the
  goal is to maximize how close each rolled segment gets to the 32MB budget
  without ever buffering more than one `batch_timeout` window's worth of
  data. Any append that pushes the raw (uncompressed) byte/event total over
  that budget short-circuits the loop instead of waiting for it: cancel the
  running timer and roll right now. Either way, rolling always starts a
  fresh loop from empty (see `handoff/2`) — there is deliberately no
  separate "a commit slot just freed up, roll early" path (see
  `handle_info({:commit_result, ...})` below): refilling a freed slot with
  whatever's pending, however small, would cut a still-growing segment's
  window short exactly when the goal is to let it keep growing. The
  threshold is sized off uncompressed bytes, not the compressed segment
  actually written to disk, so a rolled file's real log volume stays
  predictable regardless of how well any given chunk happened to compress
  (see `Logflare.Backends.Spool.Encoder`).

  Rotation, upload, and deletion: `roll/1` seals the active segment (rename)
  and opens a fresh active file. The sealed file is then handed to a `Task`
  (see `Logflare.Backends.Spool.Committer`) that this module spawns directly
  and monitors — not a persistent GenServer, so a slow or stuck GCS commit
  for one segment never blocks the next segment's commit from starting.
  Partition owns the sealed file's entire lifecycle: it creates it (roll)
  and deletes it once the task reports success — `Committer` itself never
  touches the file except to read it. Concurrent commits are bounded by
  `max_inflight_commits` (config, default 10) purely to cap how many
  segments' worth of bytes can be held in memory at once across a
  partition's outstanding uploads during a sustained backend slowdown; the
  local WAL backlog itself is cheap and already durable on disk regardless
  of how many uploads are in flight. A roll that's ready but finds no free
  slot doesn't get dropped — it just falls back to the same flush-loop timer,
  which retries the same check on its next tick.
  """

  use GenServer

  require Logger

  alias Logflare.Backends.Spool.Committer
  alias Logflare.Backends.Spool.Framing

  @max_batch_bytes 32 * 1024 * 1024
  @max_batch_count 500_000
  @default_max_inflight_commits 10

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))
  end

  @doc "Blocks the caller until the chunk this appends is durable on local disk, or fails."
  @spec append(GenServer.server(), binary(), non_neg_integer(), non_neg_integer(), timeout()) ::
          :ok | {:error, term()}
  def append(partition, segment, raw_byte_size, event_count, timeout \\ 15_000) do
    GenServer.call(partition, {:append, segment, raw_byte_size, event_count}, timeout)
  end

  @doc "Enqueues a segment fire-and-forget."
  @spec append_async(GenServer.server(), binary(), non_neg_integer(), non_neg_integer()) :: :ok
  def append_async(partition, segment, raw_byte_size, event_count) do
    GenServer.cast(partition, {:append_async, segment, raw_byte_size, event_count})
  end

  @impl GenServer
  def init(opts) do
    index = Keyword.fetch!(opts, :index)
    wal_dir = Keyword.fetch!(opts, :wal_dir)
    File.mkdir_p!(wal_dir)

    committer_config = %{
      bucket: Keyword.fetch!(opts, :bucket),
      storage_mod: Keyword.fetch!(opts, :storage_mod),
      queue_mod: Keyword.fetch!(opts, :queue_mod),
      queue_ref: Keyword.fetch!(opts, :queue_ref),
      format: Keyword.fetch!(opts, :format),
      compress: Keyword.fetch!(opts, :compress),
      compression_algorithm: Keyword.fetch!(opts, :compression_algorithm),
      index: index
    }

    active_path = active_path(wal_dir, index)
    _offset = Framing.recover!(active_path)
    {:ok, fd} = :file.open(active_path, [:append, :raw, :binary])

    state = %{
      wal_dir: wal_dir,
      index: index,
      fd: fd,
      active_path: active_path,
      batch_timeout: Keyword.fetch!(opts, :batch_timeout),
      pending_bytes: 0,
      pending_count: 0,
      timer_ref: nil,
      task_in_flight: 0,
      tasks: %{},
      committer_config: committer_config
    }

    {:ok, recover_sealed_segments(state)}
  end

  @impl GenServer
  def handle_call({:append, segment, raw_byte_size, event_count}, _from, state) do
    case write_segment(state, segment) do
      :ok ->
        state =
          state
          |> track_pending(raw_byte_size, event_count)
          |> maybe_roll_now_or_ensure_timer()

        {:reply, :ok, state}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  @impl GenServer
  def handle_cast({:append_async, segment, raw_byte_size, event_count}, state) do
    state =
      case write_segment(state, segment) do
        :ok ->
          state
          |> track_pending(raw_byte_size, event_count)
          |> maybe_roll_now_or_ensure_timer()

        {:error, reason} ->
          Logger.error("spool_partition: local WAL write failed: #{inspect(reason)}")
          state
      end

    {:noreply, state}
  end

  @impl GenServer
  def handle_info(:flush, state) do
    state = %{state | timer_ref: nil}
    state = if state.pending_count > 0, do: attempt_handoff(state, :timeout), else: state
    {:noreply, state}
  end

  # No re-check here on purpose — see the moduledoc. Whatever's pending
  # (under budget or capacity-blocked) is already covered by an already-
  # running flush-loop timer; freeing a slot doesn't need its own trigger.
  def handle_info({:commit_result, sealed_path, :ok}, state) do
    File.rm(sealed_path)

    {ref, tasks} = Map.pop(state.tasks, sealed_path)
    if ref, do: Process.demonitor(ref, [:flush])

    {:noreply, %{state | tasks: tasks, task_in_flight: state.task_in_flight - 1}}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Enum.find(state.tasks, fn {_sealed_path, task_ref} -> task_ref == ref end) do
      nil ->
        {:noreply, state}

      {sealed_path, _ref} ->
        Logger.error(
          "spool_partition: commit task crashed for #{sealed_path}, leaving it on disk " <>
            "for the next recovery scan: #{inspect(reason)}"
        )

        state = %{
          state
          | tasks: Map.delete(state.tasks, sealed_path),
            task_in_flight: state.task_in_flight - 1
        }

        {:noreply, state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp write_segment(state, segment) do
    with :ok <- :file.write(state.fd, segment) do
      :file.datasync(state.fd)
    end
  end

  defp track_pending(state, raw_byte_size, event_count) do
    %{
      state
      | pending_bytes: state.pending_bytes + raw_byte_size,
        pending_count: state.pending_count + event_count
    }
  end

  defp maybe_roll_now_or_ensure_timer(state) do
    if over_budget?(state) do
      state |> cancel_timer() |> attempt_handoff(:size)
    else
      ensure_timer(state)
    end
  end

  defp over_budget?(state) do
    state.pending_bytes >= @max_batch_bytes or state.pending_count >= @max_batch_count
  end

  defp attempt_handoff(state, trigger) do
    if state.task_in_flight < max_inflight_commits() do
      handoff(state, trigger)
    else
      # No free slot — fall back to the flush loop, which retries this same
      # check on its next tick. No separate "a slot just freed" signal is
      # needed: the next tick or the next oversized append always finds it.
      ensure_timer(state)
    end
  end

  defp cancel_timer(%{timer_ref: nil} = state), do: state

  defp cancel_timer(state) do
    Process.cancel_timer(state.timer_ref)
    %{state | timer_ref: nil}
  end

  defp ensure_timer(%{timer_ref: nil} = state) do
    ref = Process.send_after(self(), :flush, state.batch_timeout)
    %{state | timer_ref: ref}
  end

  defp ensure_timer(state), do: state

  defp max_inflight_commits do
    Application.get_env(:logflare, :spool, [])
    |> Keyword.get(:max_inflight_commits, @default_max_inflight_commits)
  end

  # Seals the active segment and hands it to a spawned commit Task, rather
  # than handing off in-memory bytes — the sealed file itself is what makes
  # the commit crash-recoverable, and what makes drain-and-delete race-free
  # against concurrent appends (those land in the fresh active file `roll/1`
  # opens). Every caller already guarantees timer_ref is nil by this point
  # (see maybe_roll_now_or_ensure_timer/1 and handle_info(:flush, ...)).
  defp handoff(state, trigger) do
    case roll(state) do
      {:ok, sealed_path, state} ->
        state = spawn_commit_task(state, sealed_path, state.pending_count, trigger)
        %{state | pending_bytes: 0, pending_count: 0}

      {:error, reason} ->
        Logger.error("spool_partition: failed to roll WAL segment: #{inspect(reason)}")
        # Pending counters are left as-is (still over budget), so the flush
        # loop's own retry (ensure_timer/1 below) re-triggers a roll attempt
        # on its next tick.
        ensure_timer(state)
    end
  end

  defp roll(state) do
    :ok = :file.close(state.fd)
    sealed_path = sealed_path(state.wal_dir, state.index)

    with :ok <- File.rename(state.active_path, sealed_path),
         {:ok, fd} <- :file.open(state.active_path, [:append, :raw, :binary]) do
      {:ok, sealed_path, %{state | fd: fd}}
    end
  end

  # Unlinked (Task.start/1, not Task.async/1) so a crashing commit never
  # takes this partition down with it — monitored instead, so a crash still
  # surfaces as a message (handled above) rather than silently stranding
  # task_in_flight.
  defp spawn_commit_task(state, sealed_path, total_count, trigger) do
    partition = self()
    config = state.committer_config

    {:ok, pid} =
      Task.start(fn -> Committer.commit(partition, sealed_path, total_count, trigger, config) end)

    ref = Process.monitor(pid)

    %{
      state
      | tasks: Map.put(state.tasks, sealed_path, ref),
        task_in_flight: state.task_in_flight + 1
    }
  end

  # Leftover sealed segments from a crash between roll/1 and a commit task
  # deleting them — nothing tracks their original event counts across a
  # restart, so they're re-derived from the file itself. Runs once at boot,
  # never on the steady-state append path; spawned unconditionally
  # (bypassing max_inflight_commits) since this is a one-time, bounded burst,
  # not steady-state throughput.
  defp recover_sealed_segments(state) do
    pattern = Path.join(state.wal_dir, "p#{state.index}-*.sealed")
    sealed_paths = Path.wildcard(pattern)

    Enum.reduce(sealed_paths, state, fn sealed_path, state ->
      spawn_commit_task(state, sealed_path, recovered_event_count(sealed_path), :recovered)
    end)
  end

  # An approximation, not a true event count: it's the number of framed
  # request-chunks in the segment, not the events inside each one (that would
  # need decompressing and parsing every chunk, which nothing on the producer
  # side otherwise does — encoding here is write-only). Fine for what this
  # feeds — telemetry/logging on a rare, crash-only path — since nothing
  # downstream (the consumer never reads the notification's event_count) is
  # correctness-sensitive to it.
  defp recovered_event_count(sealed_path) do
    case File.read(sealed_path) do
      {:ok, binary} ->
        {payloads, _valid_bytes, _rest} = Framing.decode_all(binary)
        length(payloads)

      {:error, _reason} ->
        0
    end
  end

  defp active_path(wal_dir, index), do: Path.join(wal_dir, "p#{index}.wal")

  defp sealed_path(wal_dir, index) do
    seq = :erlang.unique_integer([:positive, :monotonic])
    Path.join(wal_dir, "p#{index}-#{seq}.sealed")
  end
end
