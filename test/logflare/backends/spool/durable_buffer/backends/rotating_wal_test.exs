defmodule Logflare.Backends.Spool.DurableBuffer.Backends.RotatingWalTest do
  use ExUnit.Case, async: true

  alias DurableBuffer.WAL
  alias Logflare.Backends.Spool.DurableBuffer.Backends.RotatingWal, as: Backend
  alias Logflare.Backends.Spool.DurableBuffer.Backends.RotatingWal.Worker
  alias Logflare.Backends.Spool.Health
  alias Logflare.Test.FakeDurableBufferBackend, as: FakeInnerBackend

  # Backend.commit/4 reports to the real, global :disk Health scope on
  # every call — reset it so a failure here can't leak into another test
  # file's own Health assertions (this module runs async, i.e. before any
  # sync test module starts, so whatever it leaves behind is what the
  # first sync test to touch :disk would otherwise see).
  setup do
    on_exit(fn -> Health.report_recovery!(:disk) end)
  end

  defp wal_dir! do
    dir = Path.join(System.tmp_dir!(), "rotating_wal_test_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)
    dir
  end

  defp config(overrides \\ []) do
    test_pid = self()

    defaults = [
      wal_dir: wal_dir!(),
      max_batch_bytes: 1_000_000,
      worker_count: 2,
      inner_backend: {FakeInnerBackend, test_pid: test_pid, commit_result: :ok}
    ]

    Backend.init_config(Keyword.merge(defaults, overrides))
  end

  defp framed(payload) do
    {iodata, size} = WAL.encode(payload)
    {iodata, size}
  end

  test "commit/4 writes to the local active file and does not rotate below the threshold" do
    config = config(max_batch_bytes: 1_000_000)
    {:ok, state} = Backend.open(config, 0)

    {entry, size} = framed("hello")
    assert {:ok, state} = Backend.commit(state, entry, size, {0, 1})

    {payloads, _valid, _rest} = state.path |> File.read!() |> WAL.decode_all()
    assert payloads == ["hello"]
    refute_receive {:committed, _, _, _}, 50
  end

  test "crossing max_batch_bytes rotates and ships the sealed segment to the inner backend" do
    config = config(max_batch_bytes: 10, worker_count: 1)
    {:ok, state} = Backend.open(config, 0)

    {entry, size} = framed("hello world")
    assert {:ok, new_state} = Backend.commit(state, entry, size, {0, 1})

    # Rotated: pending_bytes reset, and the active file is fresh/empty again.
    assert new_state.pending_bytes == 0
    assert File.read!(new_state.path) == ""

    assert_receive {:committed, 0, body, ^size}
    {payloads, _valid, _rest} = WAL.decode_all(body)
    assert payloads == ["hello world"]
  end

  test "rotates once max_rotation_interval_ms elapses, even below max_batch_bytes" do
    config = config(max_batch_bytes: 1_000_000, max_rotation_interval_ms: 20, worker_count: 1)
    {:ok, state} = Backend.open(config, 0)

    {entry_a, size_a} = framed("a")
    assert {:ok, state} = Backend.commit(state, entry_a, size_a, {0, 1})
    refute_receive {:committed, _, _, _}, 10

    Process.sleep(30)

    {entry_b, size_b} = framed("b")
    assert {:ok, new_state} = Backend.commit(state, entry_b, size_b, {1, 1})

    assert new_state.pending_bytes == 0
    assert_receive {:committed, 0, body, _size}
    {payloads, _valid, _rest} = WAL.decode_all(body)
    assert payloads == ["a", "b"]
  end

  test "close/1 ships whatever hasn't rotated yet instead of leaving it stranded" do
    config = config(max_batch_bytes: 1_000_000, worker_count: 1)
    {:ok, state} = Backend.open(config, 0)

    {entry, size} = framed("tail")
    {:ok, state} = Backend.commit(state, entry, size, {0, 1})

    assert :ok = Backend.close(state)

    assert_receive {:committed, 0, body, ^size}
    {payloads, _valid, _rest} = WAL.decode_all(body)
    assert payloads == ["tail"]
  end

  test "close/1 is a no-op ship when nothing is pending" do
    config = config(worker_count: 1)
    {:ok, state} = Backend.open(config, 0)

    assert :ok = Backend.close(state)
    refute_receive {:committed, _, _, _}, 50
  end

  test "a leftover sealed segment from a crash is re-dispatched to a worker on open/2" do
    dir = wal_dir!()
    File.mkdir_p!(dir)

    {leftover, _size} = framed("leftover")
    File.write!(Path.join(dir, "p0-999.sealed"), IO.iodata_to_binary(leftover))

    config = config(wal_dir: dir, worker_count: 1)
    {:ok, _state} = Backend.open(config, 0)

    assert_receive {:committed, 0, body, _size}
    {payloads, _valid, _rest} = WAL.decode_all(body)
    assert payloads == ["leftover"]
  end

  test "each worker in the pool opens the inner backend with a distinct sub-partition index" do
    config = config(worker_count: 3)
    {:ok, state} = Backend.open(config, 5)

    # partition_index 5, worker_count 3 -> sub-indices 15, 16, 17
    assert length(state.workers) == 3

    for {worker, expected_index} <- Enum.zip(state.workers, [15, 16, 17]) do
      {entry, size} = framed("w")

      Worker.commit_segment(worker.pid, write_temp!(IO.iodata_to_binary(entry)))

      assert_receive {:committed, ^expected_index, _body, ^size}
    end
  end

  test "commit/4 replaces a dead worker and re-dispatches any sealed files left stranded" do
    config = config(max_batch_bytes: 1_000_000, worker_count: 1)
    {:ok, state} = Backend.open(config, 0)

    [worker] = state.workers

    # Simulates a segment the dead worker either never got to, or was
    # mid-flight on when it crashed — recover_sealed_segments/1's glob
    # picks up anything left on disk regardless of how it got stranded.
    {leftover, _size} = framed("leftover")
    stranded_path = Path.join(config.wal_dir, "p0-stranded.sealed")
    File.write!(stranded_path, IO.iodata_to_binary(leftover))

    ref = Process.monitor(worker.pid)
    Process.exit(worker.pid, :kill)
    assert_receive {:DOWN, ^ref, :process, _, _}

    {entry, size} = framed("next")
    assert {:ok, new_state} = Backend.commit(state, entry, size, {0, 1})

    assert [new_worker] = new_state.workers
    assert new_worker.index == worker.index
    assert new_worker.pid != worker.pid
    assert Process.alive?(new_worker.pid)

    assert_receive {:committed, 0, body, _size}
    {payloads, _valid, _rest} = WAL.decode_all(body)
    assert payloads == ["leftover"]
  end

  test "truncate/2 clears the active file" do
    config = config()
    {:ok, state} = Backend.open(config, 0)

    {entry, size} = framed("x")
    {:ok, state} = Backend.commit(state, entry, size, {0, 1})
    assert File.read!(state.path) != ""

    assert {:ok, truncated} = Backend.truncate(state, 0)
    assert File.read!(truncated.path) == ""
    assert truncated.pending_bytes == 0
  end

  test "stream/2 is not supported" do
    assert_raise RuntimeError, ~r/does not support stream\/2/, fn ->
      Backend.stream(config(), 0)
    end
  end

  defp write_temp!(contents) do
    path =
      Path.join(
        System.tmp_dir!(),
        "rotating_wal_test_seg_#{System.unique_integer([:positive])}.sealed"
      )

    File.write!(path, contents)
    on_exit(fn -> File.rm(path) end)
    path
  end
end
