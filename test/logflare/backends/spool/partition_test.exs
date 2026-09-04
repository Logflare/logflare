defmodule Logflare.Backends.Spool.PartitionTest do
  use ExUnit.Case, async: false

  import Mimic

  alias Logflare.Backends.Spool.Framing
  alias Logflare.Backends.Spool.Partition
  alias Logflare.Backends.Spool.Queue.PubSub, as: QueueMod
  alias Logflare.Backends.Spool.Storage.GCS, as: StorageMod
  alias Logflare.Backends.Spool.WriteHealth
  alias Logflare.TestUtils

  setup :set_mimic_global

  # Fresh dir per test — Partition recovers/scans wal_dir on init, so tests
  # can't share one without racing each other's leftover files.
  defp wal_dir! do
    dir =
      Path.join(System.tmp_dir!(), "spool_partition_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    dir
  end

  defp start_partition(opts) do
    defaults = [
      name: :"partition_#{System.unique_integer([:positive])}",
      index: 0,
      bucket: "test-bucket",
      batch_timeout: 60_000,
      compress: false,
      format: :ndjson,
      compression_algorithm: :gzip,
      storage_mod: StorageMod,
      queue_mod: QueueMod,
      queue_ref: nil,
      wal_dir: wal_dir!()
    ]

    opts = Keyword.merge(defaults, opts)
    pid = start_supervised!({Partition, opts}, id: opts[:name])
    {pid, opts[:name]}
  end

  defp segment(payload \\ "line\n"), do: Framing.encode_segment(payload)

  # Forces the next local WAL write to fail by closing the fd out from
  # under the partition. Writing to a closed file descriptor fails at the
  # OS level (:einval) regardless of permissions or user — unlike a
  # chmod-based simulation, which root-run CI would bypass — so this is a
  # reliable, portable way to exercise the disk-write-failure fallback.
  defp break_local_disk!(pid) do
    :sys.replace_state(pid, fn state ->
      :file.close(state.fd)
      state
    end)
  end

  defp assert_recovered_put(expected_payload, timeout) do
    receive do
      {:put, body} ->
        case Framing.decode_segments(body) do
          {:ok, [^expected_payload]} -> :ok
          _ -> assert_recovered_put(expected_payload, timeout)
        end
    after
      timeout ->
        flunk("did not receive a put containing #{inspect(expected_payload)} within #{timeout}ms")
    end
  end

  # storage_mod.put's send/2 to the test process happens *before* Partition
  # actually deletes the file on {:commit_result, _, :ok} — receiving the put
  # message doesn't mean the sealed file is gone yet. Polls instead of
  # asserting immediately after the message arrives.
  defp assert_eventually_gone(path, timeout_ms) when timeout_ms > 0 do
    if File.exists?(path) do
      Process.sleep(5)
      assert_eventually_gone(path, timeout_ms - 5)
    else
      :ok
    end
  end

  defp assert_eventually_gone(path, _timeout_ms), do: refute(File.exists?(path))

  describe "flush thresholds" do
    test "does not commit while under the byte budget" do
      stub(StorageMod, :put, fn _b, _k, _body, _opts -> {:ok, %{}} end)

      {pid, _name} = start_partition(batch_timeout: 60_000)

      assert :ok = Partition.append(pid, segment(), 10, 1)
      assert %{pending_count: 1, pending_bytes: 10} = :sys.get_state(pid)
    end

    test "commits immediately once the raw (uncompressed) byte budget is hit" do
      test_pid = self()

      stub(StorageMod, :put, fn _b, _k, body, _opts ->
        send(test_pid, {:put, body})
        {:ok, %{}}
      end)

      {pid, _name} = start_partition(batch_timeout: 60_000)

      # 32MB is the hardcoded max_batch_bytes, tracked as raw/uncompressed
      # bytes — see Backends.dispatch_to_spool_producer/1.
      assert :ok = Partition.append(pid, segment(), 32 * 1024 * 1024, 1)
      assert_receive {:put, _body}, 1000
    end

    test "commits on batch_timeout even without hitting a budget" do
      test_pid = self()

      stub(StorageMod, :put, fn _b, _k, body, _opts ->
        send(test_pid, {:put, body})
        {:ok, %{}}
      end)

      {pid, _name} = start_partition(batch_timeout: 20)

      assert :ok = Partition.append(pid, segment(), 10, 1)
      assert_receive {:put, _body}, 500
    end
  end

  describe "append/5 vs append_async/4" do
    test "append/5 blocks until the segment is durable on local disk (not until GCS/Pub-Sub commit)" do
      test_pid = self()
      # A commit that never resolves during the test — proves append/5's
      # reply doesn't wait on it, which is the whole point of the redesign:
      # durability is local-WAL-durable, not GCS-durable. batch_timeout is
      # long so the flush timer itself can't fire put during the assertion
      # window below — this is testing append/5's own blocking behavior,
      # not racing the batch flush.
      stub(StorageMod, :put, fn _b, _k, _body, _opts ->
        send(test_pid, :put_called)
        Process.sleep(:infinity)
      end)

      {pid, _name} = start_partition(batch_timeout: 60_000)

      assert :ok = Partition.append(pid, segment(), 10, 1)
      refute_receive :put_called, 50
    end

    test "append_async/4 returns immediately and still gets committed" do
      test_pid = self()

      stub(StorageMod, :put, fn _b, _k, body, _opts ->
        send(test_pid, {:put, body})
        {:ok, %{}}
      end)

      {pid, _name} = start_partition(batch_timeout: 10)

      assert :ok = Partition.append_async(pid, segment(), 10, 1)
      assert_receive {:put, _body}, 500
    end
  end

  describe "flush loop" do
    test "a straggler left pending after a big rotation waits for the flush timer, not for its commit slot to free" do
      test_pid = self()

      stub(StorageMod, :put, fn _b, _k, body, _opts ->
        # Slow enough that we can reliably append a second entry while the
        # first commit is still in flight, and that the timer below
        # (200ms) only fires well after this one settles (100ms) — so if
        # the second segment rolls, it can only be the timer's doing, not
        # the first commit's completion.
        Process.sleep(100)
        send(test_pid, {:put, body})
        {:ok, %{}}
      end)

      {pid, _name} = start_partition(batch_timeout: 200)

      # Byte-threshold-triggering: hands off immediately, moving straight to
      # task_in_flight, so the second append below genuinely lands while the
      # first commit is still running.
      assert :ok = Partition.append(pid, segment("first\n"), 32 * 1024 * 1024, 1)
      TestUtils.send_and_wait_for_handling(pid, :noop_sync)
      assert %{task_in_flight: 1, pending_count: 0} = :sys.get_state(pid)

      assert :ok = Partition.append(pid, segment("second\n"), 10, 1)
      assert %{task_in_flight: 1, pending_count: 1} = :sys.get_state(pid)

      assert_receive {:put, first_body}, 1000

      # The first commit just finished (task_in_flight back to 0) — this is
      # the moment the old "pipeline" refill would have rolled the second
      # segment immediately. Confirm it doesn't: maximizing how close each
      # segment gets to the size budget means letting a still-growing
      # segment use its full batch_timeout window, not cutting it short the
      # instant a slot frees up (see Partition's moduledoc).
      refute_receive {:put, _second_body}, 50

      assert_receive {:put, second_body}, 1000

      {:ok, [first_payload]} = Framing.decode_segments(first_body)
      {:ok, [second_payload]} = Framing.decode_segments(second_body)
      assert first_payload == "first\n"
      assert second_payload == "second\n"
    end

    test "an oversized append cancels a running timer and rolls immediately instead of waiting it out" do
      test_pid = self()

      stub(StorageMod, :put, fn _b, _k, body, _opts ->
        send(test_pid, {:put, body})
        {:ok, %{}}
      end)

      # Long enough that if the roll below actually waited for this timer,
      # the test would time out instead of passing.
      {pid, _name} = start_partition(batch_timeout: 60_000)

      assert :ok = Partition.append(pid, segment("small\n"), 10, 1)
      assert %{timer_ref: ref} = :sys.get_state(pid)
      refute is_nil(ref)

      assert :ok = Partition.append(pid, segment("big\n"), 32 * 1024 * 1024, 1)
      assert_receive {:put, body}, 1000

      # The flush loop never stops — a fresh timer is armed immediately
      # after the roll, not left nil, and it must be a genuinely new one
      # (the old tick was cancelled), not the same ref surviving.
      assert %{timer_ref: new_ref, pending_count: 0} = :sys.get_state(pid)
      refute is_nil(new_ref)
      assert new_ref != ref

      {:ok, payloads} = Framing.decode_segments(body)
      assert payloads == ["small\n", "big\n"]
    end
  end

  describe "max_inflight_commits" do
    test "a rotation over the cap still rolls immediately; only starting its upload is deferred" do
      # recovery_retry_delay_ms controls the deferred second commit's own
      # retry (see Partition's moduledoc — it's decoupled from
      # batch_timeout), set well above the refute_receive window below so
      # it can't have already fired by the time that assertion runs.
      Application.put_env(:logflare, :spool,
        max_inflight_commits: 1,
        recovery_retry_delay_ms: 300
      )

      test_pid = self()
      dir = wal_dir!()

      stub(StorageMod, :put, fn _b, _k, body, _opts ->
        send(test_pid, {:put_started, self(), body})

        receive do
          :proceed -> {:ok, %{}}
        end
      end)

      {pid, _name} = start_partition(wal_dir: dir)

      assert :ok = Partition.append(pid, segment("first\n"), 32 * 1024 * 1024, 1)
      assert_receive {:put_started, first_task, first_body}, 1000
      assert %{task_in_flight: 1} = :sys.get_state(pid)

      # Also over budget, but the cap (1) is already saturated. The active
      # file still rolls right away (pending resets to 0) — leaving it
      # un-rolled until a slot freed would let it keep absorbing appends
      # past the 32MB budget for as long as capacity stayed tight, which is
      # exactly the bug this fixes. Only *starting* this segment's upload is
      # deferred, to the same recovery-style retry a crash-recovered file
      # would use.
      assert :ok = Partition.append(pid, segment("second\n"), 32 * 1024 * 1024, 1)
      refute_receive {:put_started, _task, _body}, 100
      assert %{task_in_flight: 1, pending_count: 0} = :sys.get_state(pid)

      send(first_task, :proceed)

      assert_receive {:put_started, _second_task, second_body}, 1000

      {:ok, [first_payload]} = Framing.decode_segments(first_body)
      {:ok, [second_payload]} = Framing.decode_segments(second_body)
      assert first_payload == "first\n"
      assert second_payload == "second\n"
    end

    test "the active file never grows past one segment's worth even while capacity stays fully saturated" do
      # recovery_retry_delay_ms doesn't matter for what this test checks —
      # nothing releases the one busy slot, so a deferred rotation's own
      # retry never finds room to spawn regardless of how soon it fires.
      Application.put_env(:logflare, :spool, max_inflight_commits: 1)
      test_pid = self()
      dir = wal_dir!()

      stub(StorageMod, :put, fn _b, _k, _body, _opts ->
        send(test_pid, :put_started)

        receive do
          :proceed -> {:ok, %{}}
        end
      end)

      {pid, _name} = start_partition(wal_dir: dir)

      assert :ok = Partition.append(pid, segment("one\n"), 32 * 1024 * 1024, 1)
      assert_receive :put_started, 1000

      # Two more oversized appends while the only commit slot stays busy —
      # each must still roll into its own sealed file. Before this fix, a
      # saturated cap left the active file un-rolled, so it would just keep
      # absorbing these past the 32MB budget instead.
      assert :ok = Partition.append(pid, segment("two\n"), 32 * 1024 * 1024, 1)
      assert :ok = Partition.append(pid, segment("three\n"), 32 * 1024 * 1024, 1)

      assert %{pending_bytes: 0, pending_count: 0, active_path: active_path} =
               :sys.get_state(pid)

      assert {:ok, %{size: 0}} = File.stat(active_path)

      # "one" (uploading) + "two" and "three" (deferred, awaiting a slot).
      assert length(Path.wildcard(Path.join(dir, "p0-*.sealed"))) == 3
    end
  end

  describe "retry on commit failure" do
    test "retries in the background until it succeeds, without affecting the caller's already-returned :ok" do
      Application.put_env(:logflare, :spool, retry_delay_ms: 1)
      test_pid = self()
      {:ok, counter} = Agent.start_link(fn -> 0 end)

      stub(StorageMod, :put, fn _b, _k, _body, _opts ->
        attempt = Agent.get_and_update(counter, &{&1, &1 + 1})
        send(test_pid, {:put_attempt, attempt})
        if attempt < 2, do: {:error, :timeout}, else: {:ok, %{}}
      end)

      {pid, _name} = start_partition(batch_timeout: 10)

      # The caller already got :ok before any of this retrying happens — the
      # eventual upload outcome is invisible to it.
      assert :ok = Partition.append(pid, segment(), 10, 1)
      assert_receive {:put_attempt, 0}, 1000
      assert_receive {:put_attempt, 1}, 1000
      assert_receive {:put_attempt, 2}, 1000
    end
  end

  describe "commit task crash resilience" do
    test "a commit task that crashes leaves the sealed file on disk and does not crash the partition" do
      test_pid = self()
      dir = wal_dir!()

      stub(StorageMod, :put, fn _b, _k, _body, _opts ->
        send(test_pid, :put_called)
        raise "boom"
      end)

      {pid, _name} = start_partition(wal_dir: dir, batch_timeout: 60_000)
      ref = Process.monitor(pid)

      assert :ok = Partition.append(pid, segment(), 32 * 1024 * 1024, 1)
      assert_receive :put_called, 1000

      refute_receive {:DOWN, ^ref, :process, ^pid, _reason}, 200

      TestUtils.retry_assert(fn -> assert %{task_in_flight: 0} = :sys.get_state(pid) end)

      assert [_leftover] = Path.wildcard(Path.join(dir, "p0-*.sealed"))
    end
  end

  describe "crash recovery" do
    test "leftover sealed segments from a prior crash are uploaded on the next init" do
      test_pid = self()
      dir = wal_dir!()

      stub(StorageMod, :put, fn _b, _k, body, _opts ->
        send(test_pid, {:put, body})
        {:ok, %{}}
      end)

      # Simulates a sealed segment left behind by a process that died between
      # roll/1 sealing it and a commit task deleting it — no live Partition
      # wrote this file in this test, matching a real crash's on-disk
      # leftovers.
      leftover = Path.join(dir, "p0-999.sealed")
      File.write!(leftover, segment("recovered\n"))

      {_pid, _name} = start_partition(wal_dir: dir, batch_timeout: 60_000)

      # Filters for the specific recovered payload rather than accepting the
      # first {:put, _} that arrives — Mimic's global stub means an
      # unrelated in-flight retry from another test in this file could in
      # principle still land a {:put, _} here too, and this should hold
      # regardless of that.
      assert_recovered_put("recovered\n", 1000)
      assert_eventually_gone(leftover, 500)
    end

    test "many leftover sealed segments are drained without blocking init, never exceeding max_inflight_commits" do
      Application.put_env(:logflare, :spool, max_inflight_commits: 2)
      test_pid = self()
      dir = wal_dir!()

      leftovers =
        for n <- 1..5 do
          path = Path.join(dir, "p0-#{n}.sealed")
          File.write!(path, segment("recovered-#{n}\n"))
          path
        end

      stub(StorageMod, :put, fn _b, _k, body, _opts ->
        send(test_pid, {:put_started, self(), body})

        receive do
          :proceed -> {:ok, %{}}
        end
      end)

      # start_supervised!/init/1 must return promptly regardless of how many
      # leftover files exist — recovery is bounded and drains in the
      # background, never blocking startup (see Partition's moduledoc). The
      # recovery loop's own retry for whatever's left over runs on
      # recovery_retry_delay_ms (default 100ms, decoupled from
      # batch_timeout — see handle_info({:recover_sealed, ...})), not
      # immediately the instant a slot frees.
      {pid, _name} = start_partition(wal_dir: dir)

      # Exactly the cap (2) start immediately, not all 5.
      assert_receive {:put_started, task_a, _}, 1000
      assert_receive {:put_started, task_b, _}, 1000
      refute_receive {:put_started, _task, _body}, 30
      assert %{task_in_flight: 2} = :sys.get_state(pid)

      send(task_a, :proceed)
      assert_receive {:put_started, task_c, _}, 1000

      send(task_b, :proceed)
      assert_receive {:put_started, task_d, _}, 1000

      send(task_c, :proceed)
      assert_receive {:put_started, task_e, _}, 1000

      send(task_d, :proceed)
      send(task_e, :proceed)

      for leftover <- leftovers, do: assert_eventually_gone(leftover, 500)
    end

    test "a torn tail in the active WAL file is truncated on recovery, not treated as corruption" do
      dir = wal_dir!()
      active_path = Path.join(dir, "p0.wal")

      whole = segment("whole\n")
      torn = binary_part(segment("torn\n"), 0, 5)
      File.write!(active_path, whole <> torn)

      test_pid = self()

      stub(StorageMod, :put, fn _b, _k, body, _opts ->
        send(test_pid, {:put, body})
        {:ok, %{}}
      end)

      {pid, _name} = start_partition(wal_dir: dir, batch_timeout: 10)

      # The recovered "whole" entry isn't tracked in pending_count/bytes (no
      # in-memory metadata survives a crash — see recover_sealed_segments/1),
      # so a fresh append is what actually triggers a flush here; the torn
      # tail must have been truncated for this append to land cleanly after
      # it in the same active file.
      assert :ok = Partition.append(pid, segment("new\n"), 10, 1)
      assert_receive {:put, _body}, 1000
    end
  end

  describe "local WAL write failure fallback" do
    setup do
      on_exit(fn -> WriteHealth.report_recovery!() end)
      :ok
    end

    test "append/5 falls back to a direct GCS write when the local disk write fails, and still returns :ok" do
      test_pid = self()

      stub(StorageMod, :put, fn _b, _k, body, _opts ->
        send(test_pid, {:put, body})
        {:ok, %{}}
      end)

      {pid, _name} = start_partition(batch_timeout: 60_000)
      break_local_disk!(pid)

      assert :ok = Partition.append(pid, segment("fallback\n"), 10, 1)
      assert_receive {:put, body}, 1000
      assert {:ok, ["fallback\n"]} = Framing.decode_segments(body)
      assert WriteHealth.healthy?() == true
    end

    test "append_async/4 falls back to a direct GCS write in the background when the local disk write fails" do
      test_pid = self()

      stub(StorageMod, :put, fn _b, _k, body, _opts ->
        send(test_pid, {:put, body})
        {:ok, %{}}
      end)

      {pid, _name} = start_partition(batch_timeout: 60_000)
      break_local_disk!(pid)

      assert :ok = Partition.append_async(pid, segment("async-fallback\n"), 10, 1)
      assert_receive {:put, body}, 1000
      assert {:ok, ["async-fallback\n"]} = Framing.decode_segments(body)
    end

    test "append/5 returns an error, and marks the node unhealthy, if the GCS fallback also fails" do
      Application.put_env(:logflare, :spool, max_commit_attempts: 1)
      stub(StorageMod, :put, fn _b, _k, _body, _opts -> {:error, :timeout} end)

      {pid, _name} = start_partition(batch_timeout: 60_000)
      break_local_disk!(pid)

      assert {:error, {:disk_and_gcs_unavailable, _disk_reason, _gcs_reason}} =
               Partition.append(pid, segment("boom\n"), 10, 1)

      assert WriteHealth.healthy?() == false
    end

    test "a subsequent successful write clears the unhealthy state" do
      Application.put_env(:logflare, :spool, max_commit_attempts: 1)
      stub(StorageMod, :put, fn _b, _k, _body, _opts -> {:error, :timeout} end)

      {pid, _name} = start_partition(batch_timeout: 60_000)
      break_local_disk!(pid)

      assert {:error, _} = Partition.append(pid, segment(), 10, 1)
      assert WriteHealth.healthy?() == false

      # A fresh partition (healthy fd) succeeding should clear the flag —
      # WriteHealth is node-wide, not scoped to whichever partition tripped it.
      stub(StorageMod, :put, fn _b, _k, _body, _opts -> {:ok, %{}} end)
      {pid2, _name2} = start_partition(batch_timeout: 60_000)
      assert :ok = Partition.append(pid2, segment(), 10, 1)

      assert WriteHealth.healthy?() == true
    end

    test "emits telemetry when the local WAL write fails" do
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :wal, :write_error])
      stub(StorageMod, :put, fn _b, _k, _body, _opts -> {:ok, %{}} end)

      {pid, _name} = start_partition(batch_timeout: 60_000)
      break_local_disk!(pid)

      assert :ok = Partition.append(pid, segment(), 10, 1)

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :wal, :write_error],
                      %{count: 1}, %{index: 0}}
    end
  end
end
