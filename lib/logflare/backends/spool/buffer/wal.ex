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
  before reporting failure to `Logflare.Backends.Spool.Health` — this
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
  has to wait for this partition to restart. It's simply left on disk,
  unmarked, for `recover/1` to pick up again next time; there's no attempt
  limit or quarantine — a file that can genuinely never succeed just sits
  there until someone notices (`Partition.settle_commit/3` reports every
  commit failure to `Logflare.Backends.Spool.Health`) or manually clears it.
  """

  @behaviour Logflare.Backends.Spool.Buffer

  require Logger

  alias Logflare.Backends.Spool.Framing
  alias Logflare.Backends.Spool.Health

  @max_batch_bytes 32 * 1024 * 1024
  @max_write_retry 1

  @impl true
  def init(opts) do
    index = Keyword.fetch!(opts, :index)
    wal_dir = Keyword.fetch!(opts, :wal_dir)
    File.mkdir_p!(wal_dir)

    active_path = active_path(wal_dir, index)
    _offset = Framing.recover!(active_path)
    {:ok, fd} = :file.open(active_path, [:append, :raw, :binary])

    %{
      wal_dir: wal_dir,
      index: index,
      fd: fd,
      active_path: active_path,
      pending_bytes: 0,
      pending_count: 0
    }
  end

  @impl true
  def append(state, segment, raw_byte_size, event_count) do
    case write_segment(state, segment) do
      {:ok, state} ->
        Health.report_recovery!()
        {:ok, track_pending(state, raw_byte_size, event_count)}

      {:error, reason, state} ->
        emit_wal_write_error_telemetry(state, reason)
        Health.report_failure!()
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

  # Commit-result health reporting itself is handled uniformly, for every
  # buffer, by Partition.settle_commit/3 — not here.
  @impl true
  def on_commit_result(state, sealed_path, :ok) do
    File.rm(sealed_path)
    state
  end

  def on_commit_result(state, sealed_path, {:error, reason}) do
    Logger.error(
      "spool_buffer_wal: commit exhausted its attempts for #{sealed_path}, leaving it on " <>
        "disk for the next recovery scan: #{inspect(reason)}"
    )

    state
  end

  @impl true
  def recover(state) do
    pattern = Path.join(state.wal_dir, "p#{state.index}-*.sealed")

    items =
      for path <- Path.wildcard(pattern) do
        {fn -> File.read(path) end, path, recovered_event_count(path)}
      end

    {items, state}
  end

  # One bounded reopen-and-retry attempt on failure — see this module's doc
  # for why. `state.fd` can also already be `nil` here (a previous roll's
  # reopen failed), in which case there's nothing to retry a write against
  # until ensure_fd/1 gets a fresh one.
  #
  defp write_segment(_state, _segment, attempt \\ 0)

  defp write_segment(%{fd: nil, active_path: path} = state, segment, attempt) do
    case :file.open(path, [:append, :raw, :binary]) do
      {:ok, fd} -> write_segment(%{state | fd: fd}, segment, attempt)
      {:error, reason} -> {:error, reason, state}
    end
  end

  defp write_segment(%{fd: fd} = state, segment, attempt) do
    with {:error, reason} <- try_write(fd, segment),
         {:retry, true, _} <- {:retry, attempt < @max_write_retry, reason} do
      :file.close(state.fd)
      write_segment(%{state | fd: nil}, segment, attempt + 1)
    else
      :ok -> {:ok, state}
      {:retry, false, reason} -> {:error, reason, state}
    end
  end

  defp try_write(fd, segment) do
    with :ok <- :file.write(fd, segment) do
      :file.datasync(fd)
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
      %{},
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

    with :ok <- File.rename(state.active_path, sealed_path),
         {:ok, fd} <- :file.open(state.active_path, [:append, :raw, :binary]) do
      new_state = %{state | pending_bytes: 0, pending_count: 0, fd: fd}
      {:ok, fn -> File.read(sealed_path) end, sealed_path, state.pending_count, new_state}
    else
      {:error, reason} ->
        Health.report_failure!()
        Logger.error("spool_buffer_wal: failed to roll WAL segment: #{inspect(reason)}")
        {:error, reason, %{state | fd: nil}}
    end
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

  defp active_path(wal_dir, index), do: Path.join(wal_dir, "p#{index}.wal")

  defp sealed_path(wal_dir, index) do
    seq = :erlang.unique_integer([:positive, :monotonic])
    Path.join(wal_dir, "p#{index}-#{seq}.sealed")
  end
end
