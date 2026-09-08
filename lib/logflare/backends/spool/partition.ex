defmodule Logflare.Backends.Spool.Partition do
  @moduledoc """
  Accumulates pre-compressed, pre-framed segments pushed directly by ingest
  callers via `append/5` — no ETS, no poll interval.

  Every append is written and `datasync`ed to the active local WAL file
  before replying: durability here means local-disk-durable, not
  GCS/Pub-Sub-durable, which is what keeps this fast (an `fsync` instead of
  a network round trip).

  A `batch_timeout` timer (config, default 1s), started once in `init/1`,
  runs forever: every tick rolls whatever has accumulated, however little,
  then re-arms itself (`handle_info(:flush, ...)`). `maybe_roll/1` is the
  only other thing that touches the timer — it cancels and restarts it to
  roll early once an append's raw (uncompressed) byte total crosses the
  32MB budget, so a rolled segment's real log volume stays predictable
  regardless of how well it happened to compress (see
  `Logflare.Backends.Spool.Encoder`).

  `roll/1` seals the active file (rename) and opens a fresh one
  *unconditionally*, even with no commit slot free — deferring the roll
  itself would let the active file grow past budget for as long as uploads
  stayed backed up. Only *starting* the upload is capacity-gated
  (`start_commit_or_defer/4`, bounded by `max_inflight_commits`, config
  default 10); with no free slot it's deferred via a self-rescheduling
  `{:retry_commit, ...}` message. `Partition` owns a sealed file's entire
  lifecycle (creates it, deletes it once its commit succeeds); `Committer`
  only ever reads it.

  `init/1` must never block on `schedule_sealed_recovery/1` finishing — OTP
  registers this process's `:via` name before `init/1` runs, so a
  supervisor-driven restart can already have callers routed to it while
  recovery is still draining leftover sealed files from a crash. So
  `init/1` just sends itself one `{:recover_sealed, paths}` message and
  returns; `handle_info/2` starts as many as capacity allows and
  reschedules itself for the rest after `recovery_retry_delay_ms` (config,
  default 100ms).

  A local WAL write failure has no GCS fallback — a synchronous per-request
  GCS PUT on this hot path doesn't scale (a `Partition` is one GenServer;
  blocking every append on its own GCS PUT while disk is down backs up its
  mailbox almost immediately). Instead, a failed write or roll gets exactly
  one reopen-and-retry attempt (`reopen_active_file/1`,
  `write_with_recovery/2`) before reporting failure to
  `Logflare.Backends.Spool.WriteHealth` — this covers failures where the fd
  itself got invalidated, not just ones like `:enospc` where the same fd
  recovers on its own. `Logflare.Backends.spool_producer_mode?/0` checks
  `WriteHealth.healthy?/0`, so once that happens this node stops routing
  new ingest through spool and fails its own health check at the same
  moment, until a later successful write clears it again.
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
      {:ok, state} ->
        WriteHealth.report_recovery!()
        {:reply, :ok, state |> track_pending(raw_byte_size, event_count) |> maybe_roll()}

      {:error, reason, state} ->
        emit_wal_write_error_telemetry(state, reason)
        WriteHealth.report_failure!()

        Logger.error("spool_partition: local WAL write failed: #{inspect(reason)}")

        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_info(:flush, state) do
    state = if state.pending_count > 0, do: handoff(state, :timeout), else: state
    {:noreply, start_flush_loop(state)}
  end

  def handle_info({:commit_result, sealed_path, result}, state) do
    case result do
      {:ok, _file_key} ->
        File.rm(sealed_path)
        WriteHealth.report_recovery!()

      {:error, reason} ->
        Logger.error(
          "spool_partition: commit exhausted its attempts for #{sealed_path}, leaving it on " <>
            "disk for the next recovery scan: #{inspect(reason)}"
        )

        WriteHealth.report_failure!()
    end

    {:noreply, forget_task(state, sealed_path)}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Enum.find(state.tasks, fn {_sealed_path, task_ref} -> task_ref == ref end) do
      nil ->
        {:noreply, state}

      {sealed_path, _ref} ->
        Logger.error(
          "spool_partition: commit task crashed for #{sealed_path}, leaving it on disk for " <>
            "the next recovery scan: #{inspect(reason)}"
        )

        {:noreply, forget_task(state, sealed_path)}
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
        spawn_commit(state, path, recovered_event_count(path), :recovered)
      end)

    if remaining != [] do
      Process.send_after(self(), {:recover_sealed, remaining}, state.recovery_retry_delay_ms)
    end

    {:noreply, state}
  end

  # A single commit deferred by start_commit_or_defer/4 because no slot was
  # free at the time — retried here, one at a time, the moment this fires.
  def handle_info({:retry_commit, sealed_path, total_count, trigger}, state) do
    {:noreply, start_commit_or_defer(state, sealed_path, total_count, trigger)}
  end

  def handle_info(_message, state), do: {:noreply, state}

  # One bounded reopen-and-retry attempt on failure — see this module's doc
  # for why. `state.fd` can also already be `nil` here (a previous roll's
  # reopen failed), in which case there's nothing to retry a write against
  # until ensure_fd/1 gets a fresh one.
  defp write_segment(state, segment) do
    case ensure_fd(state) do
      {:ok, state} -> write_with_recovery(state, segment)
      {:error, reason, state} -> {:error, reason, state}
    end
  end

  defp ensure_fd(%{fd: nil} = state), do: open_active_file(state)
  defp ensure_fd(state), do: {:ok, state}

  defp write_with_recovery(state, segment) do
    case try_write(state.fd, segment) do
      :ok ->
        {:ok, state}

      {:error, reason} ->
        case reopen_active_file(state) do
          {:ok, state} ->
            case try_write(state.fd, segment) do
              :ok -> {:ok, state}
              {:error, reason} -> {:error, reason, state}
            end

          {:error, _reopen_reason, state} ->
            {:error, reason, state}
        end
    end
  end

  defp try_write(fd, segment) do
    with :ok <- :file.write(fd, segment) do
      :file.datasync(fd)
    end
  end

  # Closes whatever fd is currently held (tolerating a failure there — it's
  # already being discarded either way) and opens a fresh one at
  # active_path. Used both to recover from a write failure and by roll/1,
  # which always needs a new fd regardless of how the rename went.
  defp reopen_active_file(state) do
    case :file.close(state.fd) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning(
          "spool_partition: closing active WAL fd before reopen failed: #{inspect(reason)}"
        )
    end

    open_active_file(state)
  end

  defp open_active_file(state) do
    case :file.open(state.active_path, [:append, :raw, :binary]) do
      {:ok, fd} -> {:ok, %{state | fd: fd}}
      {:error, reason} -> {:error, reason, %{state | fd: nil}}
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
        state = start_commit_or_defer(state, sealed_path, state.pending_count, trigger)
        %{state | pending_bytes: 0, pending_count: 0}

      {:error, reason, state} ->
        WriteHealth.report_failure!()
        Logger.error("spool_partition: failed to roll WAL segment: #{inspect(reason)}")
        # Pending counters are left as-is (still over budget) — the flush
        # loop's next tick retries the roll on its own, no special-cased
        # retry needed here. state.fd has already been reopened by roll/1
        # (or, if that also failed, is nil and will be reopened lazily by
        # the next write_segment/2 call) — it's never left stale/closed.
        state
    end
  end

  # Always reopens the active file afterward, regardless of whether the
  # rename actually succeeded — see this module's doc. If the rename
  # failed, active_path still holds the not-yet-sealed segment, so
  # reopening it just resumes appending where the caller left off; if the
  # rename succeeded, active_path is gone and reopening (in :append mode)
  # creates the next segment's fresh file.
  defp roll(state) do
    case :file.close(state.fd) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning(
          "spool_partition: closing active WAL fd before roll failed: #{inspect(reason)}"
        )
    end

    sealed_path = sealed_path(state.wal_dir, state.index)
    rename_result = File.rename(state.active_path, sealed_path)

    case open_active_file(state) do
      {:ok, state} ->
        case rename_result do
          :ok -> {:ok, sealed_path, state}
          {:error, reason} -> {:error, reason, state}
        end

      {:error, open_reason, state} ->
        {:error, open_reason, state}
    end
  end

  defp start_commit_or_defer(state, sealed_path, total_count, trigger) do
    if state.task_in_flight < state.max_inflight_commits do
      spawn_commit(state, sealed_path, total_count, trigger)
    else
      Process.send_after(
        self(),
        {:retry_commit, sealed_path, total_count, trigger},
        state.recovery_retry_delay_ms
      )

      state
    end
  end

  # The one place every commit — a normal rotation or a recovered file —
  # actually starts. Unlinked (Committer.commit_async/5 uses Task.start/1)
  # so a crashing commit never takes this partition down with it; monitored
  # here instead, so a crash still surfaces as a message (handled above)
  # rather than silently stranding task_in_flight.
  defp spawn_commit(state, sealed_path, total_count, trigger) do
    {:ok, pid} =
      Committer.commit_async(self(), sealed_path, total_count, trigger, state.committer_config)

    ref = Process.monitor(pid)

    %{
      state
      | tasks: Map.put(state.tasks, sealed_path, ref),
        task_in_flight: state.task_in_flight + 1
    }
  end

  defp forget_task(state, sealed_path) do
    {ref, tasks} = Map.pop(state.tasks, sealed_path)
    if ref, do: Process.demonitor(ref, [:flush])
    %{state | tasks: tasks, task_in_flight: state.task_in_flight - 1}
  end

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
