defmodule Logflare.Backends.Spool.Buffer.WALTest do
  use ExUnit.Case, async: false

  alias Logflare.Backends.Spool.Buffer.WAL
  alias Logflare.Backends.Spool.Framing
  alias Logflare.Backends.Spool.Health
  alias Logflare.TestUtils

  setup do
    on_exit(fn -> Health.report_recovery!() end)
    :ok
  end

  defp wal_dir! do
    dir = Path.join(System.tmp_dir!(), "buffer_wal_test_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    dir
  end

  defp init!(opts \\ []) do
    wal_dir = Keyword.get(opts, :wal_dir) || wal_dir!()
    index = Keyword.get(opts, :index, 0)
    WAL.init(wal_dir: wal_dir, index: index)
  end

  defp segment(payload \\ "line\n"), do: Framing.encode_segment(payload)

  describe "init/1" do
    test "creates wal_dir if missing and opens the active file" do
      dir =
        Path.join(
          System.tmp_dir!(),
          "buffer_wal_test_missing_#{System.unique_integer([:positive])}"
        )

      refute File.dir?(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      state = WAL.init(wal_dir: dir, index: 0)

      assert File.dir?(dir)
      assert state.pending_bytes == 0
      assert state.pending_count == 0
      assert state.active_path == Path.join(dir, "p0.wal")
      assert state.fd != nil
    end

    test "truncates a torn tail left in the active file, rather than treating it as corruption" do
      dir = wal_dir!()
      active_path = Path.join(dir, "p0.wal")

      whole = segment("whole\n")
      torn = binary_part(segment("torn\n"), 0, 5)
      File.write!(active_path, whole <> torn)

      _state = WAL.init(wal_dir: dir, index: 0)

      assert {:ok, binary} = File.read(active_path)
      assert binary == whole
    end
  end

  describe "append/4" do
    test "writes and fsyncs the segment, tracking pending_bytes/pending_count across calls" do
      state = init!()

      assert {:ok, state} = WAL.append(state, segment(), 10, 1)
      assert state.pending_bytes == 10
      assert state.pending_count == 1

      assert {:ok, state} = WAL.append(state, segment(), 5, 1)
      assert state.pending_bytes == 15
      assert state.pending_count == 2

      assert {:ok, body} = File.read(state.active_path)
      assert {:ok, ["line\n", "line\n"]} = Framing.decode_segments(body)
    end

    test "reports Health recovery on every successful write" do
      Application.put_env(:logflare, :spool, max_spool_health_failures: 1)
      on_exit(fn -> Application.delete_env(:logflare, :spool) end)
      Health.report_failure!()
      assert Health.healthy?() == false

      state = init!()
      assert {:ok, _state} = WAL.append(state, segment(), 10, 1)

      assert Health.healthy?() == true
    end

    test "a write against a merely-closed fd self-heals via one reopen, without ever going unhealthy" do
      state = init!()
      :file.close(state.fd)

      assert {:ok, state} = WAL.append(state, segment(), 10, 1)
      assert state.fd != nil
      assert Health.healthy?() == true
    end

    test "an unrecoverable write failure returns an error, leaves fd nil, and marks the node unhealthy" do
      Application.put_env(:logflare, :spool, max_spool_health_failures: 1)
      on_exit(fn -> Application.delete_env(:logflare, :spool) end)

      state = init!()
      :file.close(state.fd)
      state = %{state | active_path: Path.join(state.wal_dir, "does-not-exist/p0.wal")}

      # The first write fails against the closed fd (:einval); the one-shot
      # retry's own reopen then fails too (:enoent, no such directory) — the
      # reason reported is whichever failure is actually terminal (the
      # retry's), not the original one that triggered the retry.
      assert {:error, :enoent, state} = WAL.append(state, segment(), 10, 1)
      assert state.fd == nil
      assert Health.healthy?() == false
    end

    test "emits telemetry when the write fails" do
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :wal, :write_error])

      state = init!()
      :file.close(state.fd)
      state = %{state | active_path: Path.join(state.wal_dir, "does-not-exist/p0.wal")}

      assert {:error, _reason, _state} = WAL.append(state, segment(), 10, 1)

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :wal, :write_error], %{},
                      %{index: 0}}
    end
  end

  describe "roll/2" do
    test "is a no-op when nothing is pending, regardless of force" do
      state = init!()

      assert {:no_roll, ^state} = WAL.roll(state, false)
      assert {:no_roll, ^state} = WAL.roll(state, true)
    end

    test "does not roll under the byte threshold without force" do
      state = init!()
      {:ok, state} = WAL.append(state, segment(), 10, 1)

      assert {:no_roll, ^state} = WAL.roll(state, false)
    end

    test "rolls when force: true, however little has accumulated" do
      state = init!()
      {:ok, state} = WAL.append(state, segment(), 10, 1)
      active_path = state.active_path

      assert {:ok, thunk, sealed_path, 1, new_state} = WAL.roll(state, true)
      assert new_state.pending_bytes == 0
      assert new_state.pending_count == 0
      assert sealed_path != active_path
      assert File.exists?(sealed_path)

      # A fresh active file is reopened at the same path, ready for more appends.
      assert new_state.active_path == active_path
      assert {:ok, ""} = File.read(active_path)

      assert {:ok, body} = thunk.()
      assert {:ok, ["line\n"]} = Framing.decode_segments(body)
    end

    test "rolls automatically once the 32MB threshold is crossed" do
      state = init!()
      {:ok, state} = WAL.append(state, segment(), 10, 1)
      # Faking the accumulated byte count avoids actually writing 32MB to disk.
      state = %{state | pending_bytes: 32 * 1024 * 1024}

      assert {:ok, _thunk, _sealed_path, 1, _new_state} = WAL.roll(state, false)
    end

    test "a roll whose rename fails leaves pending counters untouched and the fd nil, to be reopened lazily" do
      state = init!()
      {:ok, state} = WAL.append(state, segment(), 10, 1)
      state = %{state | wal_dir: Path.join(state.wal_dir, "does-not-exist")}

      assert {:error, :enoent, new_state} = WAL.roll(state, true)
      assert new_state.pending_bytes == 10
      assert new_state.pending_count == 1
      # Not reopened here — the next append (or the next roll attempt, via
      # the recurring flush timer) reopens it fresh through the ordinary
      # `fd: nil` path, so nothing is lost by not doing it eagerly.
      assert new_state.fd == nil
    end

    test "a roll whose rename fails reports Health failure" do
      Application.put_env(:logflare, :spool, max_spool_health_failures: 1)
      on_exit(fn -> Application.delete_env(:logflare, :spool) end)

      state = init!()
      {:ok, state} = WAL.append(state, segment(), 10, 1)
      state = %{state | wal_dir: Path.join(state.wal_dir, "does-not-exist")}

      assert {:error, :enoent, _state} = WAL.roll(state, true)
      assert Health.healthy?() == false
    end
  end

  # Commit-result Health reporting is no longer this buffer's job — see
  # Partition.settle_commit/3, tested at that (buffer-agnostic) level
  # instead. Only WAL's own bookkeeping (deleting/leaving the sealed file)
  # is tested here.
  describe "on_commit_result/3" do
    test ":ok deletes the sealed file" do
      state = init!()
      {:ok, state} = WAL.append(state, segment(), 10, 1)
      {:ok, _thunk, sealed_path, _count, state} = WAL.roll(state, true)

      _state = WAL.on_commit_result(state, sealed_path, :ok)

      refute File.exists?(sealed_path)
    end

    test "a failure leaves the sealed file on disk untouched, for recover/1 to find again next restart" do
      state = init!()
      {:ok, state} = WAL.append(state, segment(), 10, 1)
      {:ok, _thunk, sealed_path, _count, state} = WAL.roll(state, true)

      _state = WAL.on_commit_result(state, sealed_path, {:error, :timeout})

      # No renaming, no attempt marker, no quarantine — recovery only ever
      # happens once, at the next restart's init/1, so there's nothing to
      # gain from tracking attempts in between (see this module's doc).
      assert File.exists?(sealed_path)
    end
  end

  describe "recover/1" do
    test "returns no items when there's nothing sealed" do
      state = init!()
      assert {[], _state} = WAL.recover(state)
    end

    test "finds sealed files for this index, deriving their event count from the file itself" do
      dir = wal_dir!()
      File.write!(Path.join(dir, "p0-1.sealed"), segment("one\n") <> segment("two\n"))
      state = init!(wal_dir: dir)

      assert {[{thunk, path, count}], _state} = WAL.recover(state)
      assert count == 2
      assert path == Path.join(dir, "p0-1.sealed")
      assert {:ok, body} = thunk.()
      assert {:ok, ["one\n", "two\n"]} = Framing.decode_segments(body)
    end

    test "ignores sealed files belonging to a different partition index" do
      dir = wal_dir!()
      File.write!(Path.join(dir, "p1-1.sealed"), segment("other\n"))
      state = init!(wal_dir: dir, index: 0)

      assert {[], _state} = WAL.recover(state)
    end
  end
end
