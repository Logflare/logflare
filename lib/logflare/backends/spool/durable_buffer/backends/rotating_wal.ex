defmodule Logflare.Backends.Spool.DurableBuffer.Backends.RotatingWal do
  @moduledoc """
  `DurableBuffer.Backend` that commits to a local WAL file like
  `DurableBuffer.Backend.Local`, then rotates the active file to an
  immutable sealed segment once `max_batch_bytes` is crossed and hands
  the segment to an inner `DurableBuffer.Backend` (e.g.
  `Logflare.Backends.Spool.DurableBuffer.Backends.Cloud`) for its own
  commit — the fast local commit (what `DurableBuffer.append/3` blocks
  on) never waits on that.

  A small, fixed pool of `Worker`s each own one independent inner
  backend instance (opened with their own sub-partition index, so this
  is safe even for an inner backend with sequential per-commit state).
  Rotated segments are dispatched to a random worker, bounding shipping
  parallelism to the pool size without ever letting two processes touch
  the same inner_state concurrently. Leftover sealed segments from a
  crash, and any unflushed tail left in the active file, are both
  recovered on `open/2` — `DurableBuffer.WAL.recover!/1` truncates a torn
  tail in place and returns where appending should resume, and
  `recover_sealed_segments/1` re-dispatches leftover `.sealed` files to
  workers.

  Rotation is triggered by `max_batch_bytes`, or by `max_rotation_interval_ms`
  elapsing since the last rotation — whichever comes first. The time
  check runs inside `commit/4`, since that's already invoked on whatever
  cadence the owning `DurableBuffer.Partition` group-commits at — no
  separate timer needed. That only helps while commits keep happening,
  though, so a pending tail is also rotated on `close/1` (a clean
  shutdown), leaving only an unclean kill between commits as an accepted
  gap.
  """

  @behaviour DurableBuffer.Backend

  require Logger

  alias DurableBuffer.WAL
  alias Logflare.Backends.Spool.DurableBuffer.Backends.RotatingWal.Worker
  alias Logflare.Backends.Spool.Health
  alias Logflare.Backends.Spool.WalPaths

  @default_max_batch_bytes 32 * 1024 * 1024
  @default_max_rotation_interval_ms 1_000
  @default_worker_count 4

  @impl true
  def init_config(opts) do
    %{
      wal_dir: Keyword.fetch!(opts, :wal_dir),
      max_batch_bytes: Keyword.get(opts, :max_batch_bytes, @default_max_batch_bytes),
      max_rotation_interval_ms:
        Keyword.get(opts, :max_rotation_interval_ms, @default_max_rotation_interval_ms),
      fsync: Keyword.get(opts, :fsync, true),
      worker_count: Keyword.get(opts, :worker_count, @default_worker_count),
      inner_backend: DurableBuffer.Backend.normalize(Keyword.fetch!(opts, :inner_backend))
    }
  end

  @impl true
  def open(config, partition_index) do
    File.mkdir_p!(config.wal_dir)
    path = WalPaths.active(config.wal_dir, partition_index)
    {offset, _entry_count} = WAL.recover!(path)
    {:ok, fd} = :file.open(path, [:append, :raw, :binary])

    state = %{
      config: config,
      partition_index: partition_index,
      fd: fd,
      path: path,
      offset: offset,
      pending_bytes: 0,
      last_rotation_at: System.monotonic_time(:millisecond),
      workers: start_workers(config, partition_index)
    }

    recover_sealed_segments(state)

    {:ok, state}
  end

  @impl true
  def commit(state, batch, byte_size, _span) do
    with :ok <- :file.write(state.fd, batch),
         :ok <- sync(state) do
      state = %{
        state
        | offset: state.offset + byte_size,
          pending_bytes: state.pending_bytes + byte_size
      }

      state = if should_rotate?(state), do: rotate(state), else: state

      Health.report_recovery!(:disk)
      {:ok, state}
    else
      {:error, reason} ->
        Health.report_failure!(:disk)
        {:error, reason, state}
    end
  end

  @impl true
  def stream(_config, _partition_index) do
    raise "#{inspect(__MODULE__)} does not support stream/2 — consumption happens via " <>
            "the inner backend's own fan-out, not by reading this buffer back."
  end

  @impl true
  def truncate(state, _next_offset) do
    :ok = :file.close(state.fd)
    :ok = File.rm(state.path)
    {:ok, fd} = :file.open(state.path, [:append, :raw, :binary])
    {:ok, %{state | fd: fd, offset: 0, pending_bytes: 0}}
  end

  @impl true
  def close(state) do
    # Ships whatever hasn't crossed max_batch_bytes/max_rotation_interval_ms
    # yet, so a clean shutdown doesn't leave a tail waiting on traffic that
    # may not resume for a while (an unclean kill is still an accepted gap —
    # nothing runs on the way down from that).
    state = if state.pending_bytes > 0, do: rotate(state), else: state
    :ok = :file.close(state.fd)
    Enum.each(state.workers, &Worker.stop/1)
    :ok
  end

  defp sync(%{config: %{fsync: false}}), do: :ok
  defp sync(state), do: :file.datasync(state.fd)

  defp should_rotate?(state) do
    state.pending_bytes >= state.config.max_batch_bytes or
      System.monotonic_time(:millisecond) - state.last_rotation_at >=
        state.config.max_rotation_interval_ms
  end

  # Always reopens the active file afterward, regardless of whether the
  # rename succeeded, so a failed rotation never leaves a stale, closed fd.
  defp rotate(state) do
    :file.datasync(state.fd)
    :ok = :file.close(state.fd)

    sealed_path = WalPaths.sealed(state.config.wal_dir, state.partition_index)

    case File.rename(state.path, sealed_path) do
      :ok ->
        dispatch_to_worker(state, sealed_path)

      {:error, reason} ->
        Logger.error(
          "durable_buffer_rotating_wal: failed to rotate WAL segment: #{inspect(reason)}"
        )
    end

    {:ok, fd} = :file.open(state.path, [:append, :raw, :binary])

    %{
      state
      | fd: fd,
        pending_bytes: 0,
        last_rotation_at: System.monotonic_time(:millisecond)
    }
  end

  defp dispatch_to_worker(state, sealed_path) do
    state.workers
    |> Enum.random()
    |> Worker.commit_segment(sealed_path)
  end

  defp start_workers(config, partition_index) do
    {inner_module, inner_config} = config.inner_backend

    for worker_index <- 0..(config.worker_count - 1) do
      sub_partition_index = partition_index * config.worker_count + worker_index
      {:ok, pid} = Worker.start(inner_module, inner_config, sub_partition_index)
      Process.monitor(pid)
      pid
    end
  end

  defp recover_sealed_segments(state) do
    pattern = WalPaths.sealed_glob(state.config.wal_dir, state.partition_index)

    for path <- Path.wildcard(pattern) do
      dispatch_to_worker(state, path)
    end
  end
end
