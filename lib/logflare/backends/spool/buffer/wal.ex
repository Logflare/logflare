defmodule Logflare.Backends.Spool.Buffer.WAL do
  @moduledoc """
  Local-disk WAL buffer for `Partition`.

  Every append writes to the active file immediately, but the fsync that
  makes it durable is batched across appends (group commit): `append/4`
  only fsyncs once `sync_pending_bytes` crosses `sync_threshold_bytes`
  (config, default 64KB), otherwise returning `:pending` until a later
  append crosses that threshold or the next roll releases it — so a
  caller's worst-case durability latency is bounded by `batch_timeout`,
  not just `sync_threshold_bytes`. A failed write or roll gets one
  reopen-and-retry attempt before reporting failure to `Health`. `roll/2`
  always fsyncs before sealing (rename) and reopens a fresh active file
  regardless of whether the rename succeeded. `recover/1` finds sealed
  files left behind by a crash, re-deriving their event counts from disk.
  A sealed file whose commit permanently fails is left on disk for the
  next `recover/1` to pick up again.
  """

  @behaviour Logflare.Backends.Spool.Buffer

  require Logger

  alias Logflare.Backends.Spool.Framing
  alias Logflare.Backends.Spool.Health

  @max_batch_bytes 32 * 1024 * 1024
  @max_write_retry 1
  @default_sync_threshold_bytes 64 * 1024

  @impl true
  def init(opts) do
    index = Keyword.fetch!(opts, :index)
    wal_dir = Keyword.fetch!(opts, :wal_dir)
    File.mkdir_p!(wal_dir)

    spool_config = Application.get_env(:logflare, :spool, [])

    active_path = active_path(wal_dir, index)
    _offset = Framing.recover!(active_path)
    {:ok, fd} = :file.open(active_path, [:append, :raw, :binary])

    %{
      wal_dir: wal_dir,
      index: index,
      fd: fd,
      active_path: active_path,
      pending_bytes: 0,
      pending_count: 0,
      # Bytes written since the last fsync (distinct from pending_bytes/
      # pending_count, which track since the last roll).
      sync_pending_bytes: 0,
      sync_threshold_bytes:
        Keyword.get(spool_config, :sync_threshold_bytes, @default_sync_threshold_bytes)
    }
  end

  @impl true
  def append(state, segment, raw_byte_size, event_count) do
    case write_segment(state, segment) do
      {:ok, state} ->
        Health.report_recovery!()

        state =
          state |> track_pending(raw_byte_size, event_count) |> track_sync_pending(raw_byte_size)

        if state.sync_pending_bytes >= state.sync_threshold_bytes do
          :file.datasync(state.fd)
          {:ok, %{state | sync_pending_bytes: 0}}
        else
          {:pending, state}
        end

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

  # One bounded reopen-and-retry attempt on failure. state.fd may already
  # be nil here (a previous roll's reopen failed).
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

  defp try_write(fd, segment), do: :file.write(fd, segment)

  defp track_pending(state, raw_byte_size, event_count) do
    %{
      state
      | pending_bytes: state.pending_bytes + raw_byte_size,
        pending_count: state.pending_count + event_count
    }
  end

  defp track_sync_pending(state, raw_byte_size) do
    %{state | sync_pending_bytes: state.sync_pending_bytes + raw_byte_size}
  end

  defp emit_wal_write_error_telemetry(state, reason) do
    :telemetry.execute(
      [:logflare, :backends, :spool, :wal, :write_error],
      %{},
      %{reason: reason, index: state.index}
    )
  end

  # Always reopens the active file afterward, regardless of whether the
  # rename succeeded, so a failed roll never leaves a stale, closed fd.
  defp do_roll(state) do
    :file.datasync(state.fd)

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
      new_state = %{
        state
        | pending_bytes: 0,
          pending_count: 0,
          sync_pending_bytes: 0,
          fd: fd
      }

      {:ok, fn -> File.read(sealed_path) end, sealed_path, state.pending_count, new_state}
    else
      {:error, reason} ->
        Health.report_failure!()
        Logger.error("spool_buffer_wal: failed to roll WAL segment: #{inspect(reason)}")
        {:error, reason, %{state | fd: nil, sync_pending_bytes: 0}}
    end
  end

  # Approximate: counts framed chunks, not the events inside each one.
  defp recovered_event_count(path) do
    case File.read(path) do
      {:ok, binary} ->
        case Framing.decode_segments(binary) do
          {:ok, segments} -> length(segments)
          {:error, :corrupt, segments} -> length(segments)
        end

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
