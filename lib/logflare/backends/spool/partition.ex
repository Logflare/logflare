defmodule Logflare.Backends.Spool.Partition do
  @moduledoc """
  Accumulates pre-compressed, pre-framed segments pushed directly by ingest
  callers via `append/5`/`append_async/4` — no ETS, no poll interval (compare
  the old Broadway-based `ProducerPipeline`, which this replaces).

  Every append is written and `datasync`ed to a local WAL file *before*
  replying — durability is local-disk-durable, not GCS/Pub-Sub-durable, which
  is what makes this fast: a local `fsync` instead of a network round trip.

  Segments accumulate in the *active* WAL file under one continuous flush
  loop, started once in `init/1` and never stopped: a `batch_timeout` timer
  (config, default 1s) always fires and always re-arms itself (see
  `handle_info(:flush, ...)`), rolling whatever has accumulated so far,
  however little — the goal is to maximize how close each rolled segment
  gets to the 32MB budget without ever buffering more than one
  `batch_timeout` window's worth of data. Because the loop is always
  running, nothing else needs to think about arming or re-arming a timer;
  the only thing that ever touches it again is `maybe_roll/1`, which cancels
  the current tick and restarts a fresh one the moment an append pushes the
  raw (uncompressed) byte total over that budget — rolling right now instead
  of waiting for the next tick. The threshold is sized off uncompressed
  bytes, not the compressed segment actually written to disk, so a rolled
  file's real log volume stays predictable regardless of how well any given
  chunk happened to compress (see `Logflare.Backends.Spool.Encoder`).

  Rotation, upload, and deletion: `roll/1` seals the active segment (rename)
  and opens a fresh active file — *unconditionally*, regardless of whether a
  commit slot happens to be free right now. Deferring the roll itself until
  capacity frees would let the active file keep absorbing new appends past
  the size budget for as long as every slot stayed busy, defeating the
  budget entirely; the active file's size has to stay bounded no matter how
  backed up uploads are. The sealed file is then handed to
  `Logflare.Backends.Spool.Committer.commit_async/5`, which spawns its own
  `Task` — not a persistent GenServer, so a slow or stuck GCS commit for one
  segment never blocks the next segment's commit from starting. Partition
  owns the sealed file's entire lifecycle: it creates it (roll) and deletes
  it once the task reports success — `Committer` itself only reads it.
  Concurrent commits are bounded by `max_inflight_commits` (config, default
  10) purely to cap how many segments' worth of bytes can be held in memory
  at once across a partition's outstanding uploads during a sustained
  backend slowdown; the local WAL backlog itself is cheap and already
  durable on disk regardless of how many uploads are in flight. Only
  *starting* a commit is capacity-gated (`start_commit_or_defer/4`) — with no
  free slot, it's deferred one at a time via a self-rescheduling
  `{:retry_commit, ...}` message instead of blocking anything.

  `init/1` (see `schedule_sealed_recovery/1`) must never block waiting for
  recovered segments to finish uploading, even though a crash can leave many
  of them behind. On a supervisor-driven restart of just this partition —
  the only realistic time recovery runs with the rest of the app already
  serving traffic — Erlang/OTP registers this process's `:via` name *before*
  `init/1` runs, so `PartitionSupervisor.random_partition/0` can already
  select and route to it while recovery is still in progress; a blocking
  recovery would make any `append/5` call routed there queue behind it and
  risk that caller's timeout. (On a cold full-node boot this can't happen at
  all — nothing accepts ingest traffic until every partition's `init/1`,
  this one included, has already returned — but the restart case is real
  and this module can't tell the two apart.) So instead `init/1` just sends
  itself one `{:recover_sealed, paths}` message and returns immediately;
  `handle_info/2` starts as many as capacity allows and, if any are left
  over, reschedules itself with the remainder after `recovery_retry_delay_ms`
  (config, default 100ms — deliberately short and independent of
  `batch_timeout`, since this is "check again for a free slot soon", not
  "wait for a batch window").

  A local WAL write failing (disk full, I/O error) is handled differently
  from every other failure here: there's no local copy to fall back on, so
  instead of erroring out this falls back to committing just that one
  segment directly — a `{:body, segment}` source instead of a `{:file, _}`
  one, going through the exact same `spawn_commit/4` /
  `start_commit_or_defer/4` machinery as a normal rotation, just bypassing
  the WAL and its 32MB batching (this one segment becomes its own small
  object). `append/5` uses `Committer.commit/4` directly and blocks on it,
  since it's already blocking a caller with its own timeout; `append_async/4`
  has no caller waiting, so it goes through `spawn_commit/4` like everything
  else. Either way, `Logflare.Backends.Spool.WriteHealth` is told about it:
  every settled commit reports success or failure, and a real double
  failure (local disk down and the synchronous GCS fallback also
  exhausting its attempts) marks the node unhealthy so its health check
  stops routing new traffic there — self-healing the moment any subsequent
  commit succeeds.
  """

  use GenServer

  require Logger

  alias Logflare.Backends.Spool.Committer
  alias Logflare.Backends.Spool.Framing
  alias Logflare.Backends.Spool.WriteHealth

  @max_batch_bytes 32 * 1024 * 1024
  @default_max_inflight_commits 10
  # Deliberately decoupled from batch_timeout — recovery retrying is "check
  # again for a free slot", not "wait for a batch window", so it shouldn't
  # inherit batch_timeout's (config, up to a few seconds) cadence.
  @default_recovery_retry_delay_ms 100

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))
  end

  @doc "Blocks the caller until the chunk this appends is durable, or fails."
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

    spool_config = Application.get_env(:logflare, :spool, [])

    state = %{
      wal_dir: wal_dir,
      index: index,
      fd: fd,
      active_path: active_path,
      batch_timeout: Keyword.fetch!(opts, :batch_timeout),
      max_inflight_commits:
        Keyword.get(spool_config, :max_inflight_commits, @default_max_inflight_commits),
      recovery_retry_delay_ms:
        Keyword.get(spool_config, :recovery_retry_delay_ms, @default_recovery_retry_delay_ms),
      pending_bytes: 0,
      pending_count: 0,
      timer_ref: nil,
      task_in_flight: 0,
      tasks: %{},
      committer_config: committer_config
    }

    state = start_flush_loop(state)
    schedule_sealed_recovery(state)
    {:ok, state}
  end

  @impl GenServer
  def handle_call({:append, segment, raw_byte_size, event_count}, _from, state) do
    case write_segment(state, segment) do
      :ok ->
        WriteHealth.report_recovery!()
        {:reply, :ok, state |> track_pending(raw_byte_size, event_count) |> maybe_roll()}

      {:error, reason} ->
        emit_wal_write_error_telemetry(state, reason)

        Logger.error(
          "spool_partition: local WAL write failed, falling back to a direct GCS write: " <>
            "#{inspect(reason)}"
        )

        case Committer.commit(
               {:body, segment},
               event_count,
               :disk_fallback,
               state.committer_config
             ) do
          {:ok, _file_key} ->
            WriteHealth.report_recovery!()
            {:reply, :ok, state}

          {:error, fallback_reason} ->
            WriteHealth.report_failure!()

            Logger.error(
              "spool_partition: direct GCS fallback also failed — disk and GCS both " <>
                "unavailable: #{inspect(fallback_reason)}"
            )

            {:reply, {:error, {:disk_and_gcs_unavailable, reason, fallback_reason}}, state}
        end
    end
  end

  @impl GenServer
  def handle_cast({:append_async, segment, raw_byte_size, event_count}, state) do
    state =
      case write_segment(state, segment) do
        :ok ->
          WriteHealth.report_recovery!()
          state |> track_pending(raw_byte_size, event_count) |> maybe_roll()

        {:error, reason} ->
          emit_wal_write_error_telemetry(state, reason)

          Logger.error(
            "spool_partition: local WAL write failed, falling back to a direct GCS write: " <>
              "#{inspect(reason)}"
          )

          spawn_commit(state, {:body, segment}, event_count, :disk_fallback)
      end

    {:noreply, state}
  end

  @impl GenServer
  def handle_info(:flush, state) do
    state = if state.pending_count > 0, do: handoff(state, :timeout), else: state
    {:noreply, start_flush_loop(state)}
  end

  def handle_info({:commit_result, source, result}, state) do
    case result do
      {:ok, _file_key} ->
        if match?({:file, _}, source), do: delete_source_file(source)
        WriteHealth.report_recovery!()

      {:error, reason} ->
        Logger.error(
          "spool_partition: commit exhausted its attempts for #{inspect(source)}, giving up: " <>
            "#{inspect(reason)}"
        )

        WriteHealth.report_failure!()
    end

    {:noreply, forget_task(state, source)}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Enum.find(state.tasks, fn {_source, task_ref} -> task_ref == ref end) do
      nil ->
        {:noreply, state}

      {source, _ref} ->
        Logger.error(
          "spool_partition: commit task crashed for #{inspect(source)}, leaving any file on " <>
            "disk for the next recovery scan: #{inspect(reason)}"
        )

        {:noreply, forget_task(state, source)}
    end
  end

  # Bounded, self-rescheduling recovery loop, entirely decoupled from the
  # live append/commit flow above — see schedule_sealed_recovery/1 (started
  # once, from init/1) and this module's doc for why it can never block
  # startup. Starts as many of `paths` as the current capacity allows, then
  # — if any are left over — reschedules itself with the remainder after
  # recovery_retry_delay_ms.
  def handle_info({:recover_sealed, paths}, state) do
    available = max(state.max_inflight_commits - state.task_in_flight, 0)
    {to_start_now, remaining} = Enum.split(paths, available)

    state =
      Enum.reduce(to_start_now, state, fn path, state ->
        spawn_commit(state, {:file, path}, recovered_event_count(path), :recovered)
      end)

    if remaining != [] do
      Process.send_after(self(), {:recover_sealed, remaining}, state.recovery_retry_delay_ms)
    end

    {:noreply, state}
  end

  # A single commit deferred by start_commit_or_defer/4 because no slot was
  # free at the time — retried here, one at a time, the moment this fires.
  def handle_info({:retry_commit, source, total_count, trigger}, state) do
    {:noreply, start_commit_or_defer(state, source, total_count, trigger)}
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

  defp maybe_roll(state) do
    if state.pending_bytes >= @max_batch_bytes do
      if state.timer_ref, do: Process.cancel_timer(state.timer_ref)
      state |> handoff(:size) |> start_flush_loop()
    else
      state
    end
  end

  defp start_flush_loop(state) do
    ref = Process.send_after(self(), :flush, state.batch_timeout)
    %{state | timer_ref: ref}
  end

  defp emit_wal_write_error_telemetry(state, reason) do
    :telemetry.execute(
      [:logflare, :backends, :spool, :wal, :write_error],
      %{count: 1},
      %{reason: reason, index: state.index}
    )
  end

  # Seals the active segment and hands it off, rather than handing off
  # in-memory bytes — the sealed file itself is what makes the commit
  # crash-recoverable, and what makes drain-and-delete race-free against
  # concurrent appends (those land in the fresh active file `roll/1`
  # opens). Rolling itself always happens, regardless of whether a commit
  # slot is currently free — see this module's doc for why. Only
  # *starting the upload* is capacity-gated; see start_commit_or_defer/4.
  defp handoff(state, trigger) do
    case roll(state) do
      {:ok, sealed_path, state} ->
        state = start_commit_or_defer(state, {:file, sealed_path}, state.pending_count, trigger)
        %{state | pending_bytes: 0, pending_count: 0}

      {:error, reason} ->
        Logger.error("spool_partition: failed to roll WAL segment: #{inspect(reason)}")
        # Pending counters are left as-is (still over budget) — the flush
        # loop's next tick retries the roll on its own, no special-cased
        # retry needed here.
        state
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

  defp start_commit_or_defer(state, source, total_count, trigger) do
    if state.task_in_flight < state.max_inflight_commits do
      spawn_commit(state, source, total_count, trigger)
    else
      Process.send_after(
        self(),
        {:retry_commit, source, total_count, trigger},
        state.recovery_retry_delay_ms
      )

      state
    end
  end

  # The one place every kind of commit — a normal rotation, a recovered
  # file, or the async disk-fallback — actually starts. Unlinked
  # (Committer.commit_async/5 uses Task.start/1) so a crashing commit never
  # takes this partition down with it; monitored here instead, so a crash
  # still surfaces as a message (handled above) rather than silently
  # stranding task_in_flight.
  defp spawn_commit(state, source, total_count, trigger) do
    {:ok, pid} =
      Committer.commit_async(self(), source, total_count, trigger, state.committer_config)

    ref = Process.monitor(pid)

    %{
      state
      | tasks: Map.put(state.tasks, source, ref),
        task_in_flight: state.task_in_flight + 1
    }
  end

  defp forget_task(state, source) do
    {ref, tasks} = Map.pop(state.tasks, source)
    if ref, do: Process.demonitor(ref, [:flush])
    %{state | tasks: tasks, task_in_flight: state.task_in_flight - 1}
  end

  defp delete_source_file({:file, path}), do: File.rm(path)

  # Leftover sealed segments from a crash between roll/1 and a commit task
  # deleting them — nothing tracks their original event counts across a
  # restart, so they're re-derived from the file itself (recovered_event_count/1).
  # Runs once at boot (or on a supervisor-driven restart of just this
  # partition — see this module's doc for why init/1 must never block
  # here). Only kicks off the self-rescheduling handle_info({:recover_sealed,
  # ...}) loop above — actually spawning commits happens entirely there,
  # bounded by capacity, so a crash that left behind arbitrarily many sealed
  # files can never spike memory with one concurrent upload per leftover
  # file.
  defp schedule_sealed_recovery(state) do
    pattern = Path.join(state.wal_dir, "p#{state.index}-*.sealed")

    case Path.wildcard(pattern) do
      [] -> :ok
      paths -> send(self(), {:recover_sealed, paths})
    end
  end

  # An approximation, not a true event count: it's the number of framed
  # request-chunks in the segment, not the events inside each one (that would
  # need decompressing and parsing every chunk, which nothing on the producer
  # side otherwise does — encoding here is write-only). Fine for what this
  # feeds — telemetry/logging on a rare, crash-only path — since nothing
  # downstream (the consumer never reads the notification's event_count) is
  # correctness-sensitive to it.
  defp recovered_event_count(path) do
    case File.read(path) do
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
