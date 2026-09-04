defmodule Logflare.Backends.Spool.PartitionTest do
  use ExUnit.Case, async: false

  import Mimic

  alias Logflare.Backends.Spool.Framing
  alias Logflare.Backends.Spool.Partition
  alias Logflare.Backends.Spool.Queue.PubSub, as: QueueMod
  alias Logflare.Backends.Spool.Storage.GCS, as: StorageMod
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
    test "does not commit while under the byte/count budget" do
      stub(StorageMod, :put, fn _b, _k, _body, _opts -> {:ok, %{}} end)

      {pid, _name} = start_partition(batch_timeout: 60_000)

      assert :ok = Partition.append(pid, segment(), 10, 1)
      assert %{pending_count: 1, pending_bytes: 10} = :sys.get_state(pid)
    end

    test "commits immediately once the event-count budget is hit" do
      test_pid = self()

      stub(StorageMod, :put, fn _b, _k, body, _opts ->
        send(test_pid, {:put, body})
        {:ok, %{}}
      end)

      {pid, _name} = start_partition(batch_timeout: 60_000)

      # 500_000 is the hardcoded max_batch_count.
      assert :ok = Partition.append(pid, segment(), 10, 500_000)
      assert_receive {:put, _body}, 1000
      assert %{pending_count: 0, pending_bytes: 0} = :sys.get_state(pid)
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
      assert %{timer_ref: nil, pending_count: 0} = :sys.get_state(pid)

      {:ok, payloads} = Framing.decode_segments(body)
      assert payloads == ["small\n", "big\n"]
    end
  end

  describe "max_inflight_commits" do
    test "bounds concurrent commit tasks; a rotation over the cap waits for the flush loop to retry once a slot frees" do
      Application.put_env(:logflare, :spool, max_inflight_commits: 1)
      test_pid = self()

      stub(StorageMod, :put, fn _b, _k, body, _opts ->
        send(test_pid, {:put_started, self(), body})

        receive do
          :proceed -> {:ok, %{}}
        end
      end)

      # Short — the capacity-blocked second rotation isn't retried the
      # instant its slot frees (see Partition's moduledoc), only on the
      # flush loop's next tick, so this needs to actually fire within the
      # test's own assertion windows below.
      {pid, _name} = start_partition(batch_timeout: 50)

      assert :ok = Partition.append(pid, segment("first\n"), 32 * 1024 * 1024, 1)
      assert_receive {:put_started, first_task, first_body}, 1000
      assert %{task_in_flight: 1} = :sys.get_state(pid)

      # Also over budget, but the cap (1) is already saturated — must not
      # spawn a second concurrent upload yet, even across several of the
      # flush loop's own retry ticks (batch_timeout: 50, refuted for 100ms).
      assert :ok = Partition.append(pid, segment("second\n"), 32 * 1024 * 1024, 1)
      refute_receive {:put_started, _task, _body}, 100
      assert %{task_in_flight: 1, pending_count: 1} = :sys.get_state(pid)

      send(first_task, :proceed)

      assert_receive {:put_started, _second_task, second_body}, 1000

      {:ok, [first_payload]} = Framing.decode_segments(first_body)
      {:ok, [second_payload]} = Framing.decode_segments(second_body)
      assert first_payload == "first\n"
      assert second_payload == "second\n"
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
end
