defmodule Logflare.Backends.Spool.Buffer.WAL do
  @moduledoc """
  Local-disk WAL buffer for `Logflare.Backends.Spool.Partition` — every
  append is written and `datasync`ed before `Partition` can reply to an
  `append/5` caller, so durability here means local-disk-durable, not
  GCS/Pub-Sub-durable, which is what keeps that call fast (an `fsync`
  instead of a network round trip).

  A local WAL write failure has no GCS fallback — a synchronous per-request
  GCS PUT on this hot path doesn't scale (blocking every append on its own
  GCS PUT while disk is down would back up a `Partition`'s mailbox almost
  immediately). Instead, a failed write or roll gets exactly one
  reopen-and-retry attempt (`reopen_active_file/1`, `write_with_recovery/2`)
  before reporting failure to `Logflare.Backends.Spool.WriteHealth` — this
  covers failures where the fd itself got invalidated, not just ones like
  `:enospc` where the same fd recovers on its own.

  `roll/2` seals the active file (rename) and opens a fresh one — always
  reopens regardless of whether the rename actually succeeded, so a failed
  roll never leaves this buffer stuck holding a stale, already-closed fd.
  `recover/1` finds leftover sealed files from a crash — nothing tracks
  their original event counts across a restart, so they're re-derived from
  the file itself.

  A sealed file whose commit permanently fails (its internal
  `max_commit_attempts` retries exhausted, or its body/frames turned out to
  be unreadable/corrupt — see `Logflare.Backends.Spool.Committer`) is never
  retried in-process — recovery only happens once, in `init/1` — so a retry
  has to wait for this partition to restart. `on_commit_result/3` tracks how
  many times that's happened by renaming the file to embed an attempt
  counter (e.g. `p0-1.attempt1.sealed`); once a file reaches
  `max_recovery_attempts` (config, default 3) without ever succeeding —
  which for corrupt/unreadable data can never happen no matter how many
  times it's retried — it's renamed once more, to a `.quarantined` suffix
  that `recover/1`'s glob no longer matches, and left on disk for manual
  inspection rather than silently retried (or deleted) forever.
  """

  @behaviour Logflare.Backends.Spool.Buffer

  require Logger

  alias Logflare.Backends.Spool.Framing
  alias Logflare.Backends.Spool.WriteHealth

  @max_batch_bytes 32 * 1024 * 1024
  @default_max_recovery_attempts 3

  @impl true
  def init(opts) do
    index = Keyword.fetch!(opts, :index)
    wal_dir = Keyword.fetch!(opts, :wal_dir)
    File.mkdir_p!(wal_dir)

    active_path = active_path(wal_dir, index)
    _offset = Framing.recover!(active_path)
    {:ok, fd} = :file.open(active_path, [:append, :raw, :binary])

    spool_config = Application.get_env(:logflare, :spool, [])

    %{
      wal_dir: wal_dir,
      index: index,
      fd: fd,
      active_path: active_path,
      pending_bytes: 0,
      pending_count: 0,
      max_recovery_attempts:
        Keyword.get(spool_config, :max_recovery_attempts, @default_max_recovery_attempts)
    }
  end

  @impl true
  def append(state, segment, raw_byte_size, event_count) do
    case write_segment(state, segment) do
      {:ok, state} ->
        WriteHealth.report_recovery!()
        {:ok, track_pending(state, raw_byte_size, event_count)}

      {:error, reason, state} ->
        emit_wal_write_error_telemetry(state, reason)
        WriteHealth.report_failure!()
        Logger.error("spool_buffer_wal: local WAL write failed: #{inspect(reason)}")
        {:error, reason, state}
    end
  end

  @impl true
  def roll(%{pending_count: 0} = state, _force), do: {:no_roll, state}

  def roll(state, force) do
    if force or state.pending_bytes >= @max_batch_bytes do
      do_roll(state)
    else
      {:no_roll, state}
    end
  end

  @impl true
  def on_commit_result(state, sealed_path, :ok) do
    File.rm(sealed_path)
    WriteHealth.report_recovery!()
    state
  end

  def on_commit_result(state, sealed_path, {:error, reason}) do
    WriteHealth.report_failure!()
    next_attempt = attempt_count(sealed_path) + 1

    if next_attempt >= state.max_recovery_attempts do
      quarantine!(sealed_path, state.index, reason)
    else
      retry_wal_commit(sealed_path, next_attempt, state.max_recovery_attempts, reason)
    end

    state
  end

  @impl true
  def recover(state) do
    pattern = Path.join(state.wal_dir, "p#{state.index}-*.sealed")

    {retryable, exhausted} =
      Path.wildcard(pattern)
      |> Enum.split_with(&(attempt_count(&1) < state.max_recovery_attempts))

    Enum.each(exhausted, &quarantine!(&1, state.index, :max_recovery_attempts_exceeded))

    items =
      for path <- retryable do
        {fn -> File.read(path) end, path, recovered_event_count(path)}
      end

    {items, state}
  end

  # One bounded reopen-and-retry attempt on failure — see this module's doc
  # for why. `state.fd` can also already be `nil` here (a previous roll's
  # reopen failed), in which case there's nothing to retry a write against
  # until ensure_fd/1 gets a fresh one.
  defp write_segment(state, segment) do
    with {:ok, state} <- ensure_fd(state),
         :ok <- try_write(state.fd, segment) do
      {:ok, state}
    else
      {:error, _reason, _state} = error -> error
      {:error, reason} -> retry_after_reopen(state, segment, reason)
    end
  end

  defp ensure_fd(%{fd: nil} = state), do: open_active_file(state)
  defp ensure_fd(state), do: {:ok, state}

  defp retry_after_reopen(state, segment, original_reason) do
    with {:ok, state} <- reopen_active_file(state),
         :ok <- try_write(state.fd, segment) do
      {:ok, state}
    else
      {:error, _reopen_reason, reopen_failed_state} ->
        {:error, original_reason, reopen_failed_state}

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  defp try_write(fd, segment) do
    with :ok <- :file.write(fd, segment) do
      :file.datasync(fd)
    end
  end

  # Closes whatever fd is currently held (tolerating a failure there — it's
  # already being discarded either way) and opens a fresh one at
  # active_path. Used both to recover from a write failure and by
  # do_roll/1, which always needs a new fd regardless of how the rename
  # went.
  defp reopen_active_file(state) do
    case :file.close(state.fd) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning(
          "spool_buffer_wal: closing active WAL fd before reopen failed: #{inspect(reason)}"
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

  defp emit_wal_write_error_telemetry(state, reason) do
    :telemetry.execute(
      [:logflare, :backends, :spool, :wal, :write_error],
      %{count: 1},
      %{reason: reason, index: state.index}
    )
  end

  # Always reopens the active file afterward, regardless of whether the
  # rename actually succeeded — see this module's doc. If the rename
  # failed, active_path still holds the not-yet-sealed segment, so
  # reopening it just resumes appending where the caller left off; if the
  # rename succeeded, active_path is gone and reopening (in :append mode)
  # creates the next segment's fresh file.
  defp do_roll(state) do
    case :file.close(state.fd) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning(
          "spool_buffer_wal: closing active WAL fd before roll failed: #{inspect(reason)}"
        )
    end

    sealed_path = sealed_path(state.wal_dir, state.index)
    rename_result = File.rename(state.active_path, sealed_path)

    case open_active_file(state) do
      {:ok, reopened_state} ->
        finish_roll(reopened_state, sealed_path, rename_result)

      {:error, open_reason, reopened_state} ->
        WriteHealth.report_failure!()
        Logger.error("spool_buffer_wal: failed to roll WAL segment: #{inspect(open_reason)}")
        {:error, open_reason, reopened_state}
    end
  end

  defp finish_roll(reopened_state, sealed_path, :ok) do
    total_count = reopened_state.pending_count
    new_state = %{reopened_state | pending_bytes: 0, pending_count: 0}
    {:ok, fn -> File.read(sealed_path) end, sealed_path, total_count, new_state}
  end

  # Rename failed — the segment is still sitting, unrolled, in the (now
  # reopened) active file, so pending_bytes/count are left untouched and
  # get retried on the next roll attempt.
  defp finish_roll(reopened_state, _sealed_path, {:error, reason}) do
    WriteHealth.report_failure!()
    Logger.error("spool_buffer_wal: failed to roll WAL segment: #{inspect(reason)}")
    {:error, reason, reopened_state}
  end

  # An approximation, not a true event count: it's the number of framed
  # request-chunks in the segment, not the events inside each one (that
  # would need decompressing and parsing every chunk, which nothing on the
  # producer side otherwise does — encoding here is write-only). Fine for
  # what this feeds — telemetry/logging on a rare, crash-only path — since
  # nothing downstream (the consumer never reads the notification's
  # event_count) is correctness-sensitive to it.
  defp recovered_event_count(path) do
    case File.read(path) do
      {:ok, binary} ->
        {payloads, _valid_bytes, _rest} = Framing.decode_all(binary)
        length(payloads)

      {:error, _reason} ->
        0
    end
  end

  # Renames the sealed file to embed the attempt it's about to become —
  # picked back up by the next recover/1 scan (this partition's next
  # restart), so "delay between retries" here is however long until that
  # happens, not an in-process timer.
  defp retry_wal_commit(sealed_path, next_attempt, max_recovery_attempts, reason) do
    retry_path = next_attempt_path(sealed_path, next_attempt)

    case File.rename(sealed_path, retry_path) do
      :ok ->
        Logger.error(
          "spool_buffer_wal: commit failed (attempt #{next_attempt}/#{max_recovery_attempts}), " <>
            "will retry #{retry_path} on next recovery scan: #{inspect(reason)}"
        )

      {:error, rename_reason} ->
        Logger.error(
          "spool_buffer_wal: commit failed and could not mark #{sealed_path} for retry " <>
            "(#{inspect(rename_reason)}) — it stays at attempt #{next_attempt - 1} and will be " <>
            "retried again as-is: #{inspect(reason)}"
        )
    end
  end

  # Terminal state: max_recovery_attempts have all failed (very likely
  # unrecoverable — see this module's doc), or the config was lowered below
  # an attempt count already reached. Renamed out of recover/1's glob rather
  # than deleted, so no commit failure ever silently loses data — it just
  # stops being retried, left on disk for manual inspection.
  defp quarantine!(sealed_path, index, reason) do
    quarantined_path = sealed_path <> ".quarantined"

    case File.rename(sealed_path, quarantined_path) do
      :ok ->
        Logger.error(
          "spool_buffer_wal: #{sealed_path} exhausted its recovery attempts, quarantined at " <>
            "#{quarantined_path} for manual inspection: #{inspect(reason)}"
        )

      {:error, rename_reason} ->
        Logger.error(
          "spool_buffer_wal: #{sealed_path} exhausted its recovery attempts but could not be " <>
            "quarantined (#{inspect(rename_reason)}) — it will be picked up again by the next " <>
            "recovery scan: #{inspect(reason)}"
        )
    end

    :telemetry.execute(
      [:logflare, :backends, :spool, :wal, :quarantined],
      %{count: 1},
      %{index: index, reason: reason}
    )
  end

  defp attempt_count(path) do
    case Regex.run(~r/\.attempt(\d+)\.sealed$/, path) do
      [_, n] -> String.to_integer(n)
      nil -> 0
    end
  end

  defp next_attempt_path(path, next_attempt) do
    String.replace(path, ~r/(\.attempt\d+)?\.sealed$/, ".attempt#{next_attempt}.sealed")
  end

  defp active_path(wal_dir, index), do: Path.join(wal_dir, "p#{index}.wal")

  defp sealed_path(wal_dir, index) do
    seq = :erlang.unique_integer([:positive, :monotonic])
    Path.join(wal_dir, "p#{index}-#{seq}.sealed")
  end
end
