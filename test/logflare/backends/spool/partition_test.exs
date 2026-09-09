defmodule Logflare.Backends.Spool.PartitionTest do
  @moduledoc """
  Tests generic `Partition` mechanics — reply timing, flush-loop/timer
  behavior, `max_inflight_commits` capacity gating, retry-on-failure, crash
  resilience, group commit — none of which branch on which `Buffer` is
  plugged in, so these run against `Buffer.Mem` (no real disk I/O, and its
  byte threshold is trivially configurable via `mem_max_batch_bytes`).

  Buffer-specific behavior (WAL's disk writes/rolls/recovery-attempt
  limits, Mem's in-memory accumulation/threshold specifics) belongs in
  `Logflare.Backends.Spool.Buffer.WALTest` / `Buffer.MemTest` instead — unit
  tests of the buffer modules directly, with no `Partition` involved.

  The one exception is "crash recovery" below, backed by `Buffer.WAL`
  instead — `Buffer.Mem.recover/1` always returns `{[], state}` (nothing
  durable to find), so the only way to exercise `Partition`'s recovery
  *orchestration* (bounded draining via `max_inflight_commits`, never
  blocking `init/1`) end-to-end is through a buffer that actually has
  leftover work to recover.
  """

  use ExUnit.Case, async: false

  import Mimic

  alias Logflare.Backends.Spool.Buffer
  alias Logflare.Backends.Spool.Framing
  alias Logflare.Backends.Spool.Health
  alias Logflare.Backends.Spool.Partition
  alias Logflare.Backends.Spool.Queue.PubSub, as: QueueMod
  alias Logflare.Backends.Spool.Storage.GCS, as: StorageMod
  alias Logflare.TestUtils

  setup :set_mimic_global

  defp start_partition(opts \\ []) do
    defaults = [
      name: :"partition_#{System.unique_integer([:positive])}",
      buffer_mod: Buffer.Mem,
      index: 0,
      bucket: "test-bucket",
      batch_timeout: 60_000,
      compress: false,
      format: :ndjson,
      compression_algorithm: :gzip,
      storage_mod: StorageMod,
      queue_mod: QueueMod,
      queue_ref: nil
    ]

    opts = Keyword.merge(defaults, opts)
    pid = start_supervised!({Partition, opts}, id: opts[:name])
    {pid, opts[:name]}
  end

  # Fresh dir per test — the WAL buffer recovers/scans wal_dir on init, so
  # tests can't share one without racing each other's leftover files. Only
  # used by the "crash recovery" describe block below.
  defp wal_dir! do
    dir =
      Path.join(System.tmp_dir!(), "spool_partition_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    dir
  end

  defp segment(payload \\ "line\n"), do: Framing.encode_segment(payload)

  defp buffer_state(pid), do: :sys.get_state(pid).buffer_state

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
  # actually deletes the file on {:commit_success, _} — receiving the put
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
      assert %{pending_count: 1, pending_bytes: 10} = buffer_state(pid)
    end

    test "commits immediately once the raw (uncompressed) byte budget is hit" do
      test_pid = self()

      stub(StorageMod, :put, fn _b, _k, body, _opts ->
        send(test_pid, {:put, body})
        {:ok, %{}}
      end)

      Application.put_env(:logflare, :spool, mem_max_batch_bytes: 10)
      on_exit(fn -> Application.delete_env(:logflare, :spool) end)

      {pid, _name} = start_partition(batch_timeout: 60_000)

      assert :ok = Partition.append(pid, segment(), 10, 1)
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

  describe "append/5" do
    test "blocks until the segment is durable in the buffer (not until GCS/Pub-Sub commit)" do
      test_pid = self()
      # A commit that never resolves during the test — proves append/5's
      # reply doesn't wait on it, which is the whole point of the redesign:
      # durability is buffer-durable, not GCS-durable. batch_timeout is long
      # so the flush timer itself can't fire put during the assertion
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
  end

  describe "append/5 with wait_until_committed: true" do
    test "blocks until the batch is actually committed, unlike the default" do
      test_pid = self()

      stub(StorageMod, :put, fn _b, _k, body, _opts ->
        send(test_pid, {:put, body})
        {:ok, %{}}
      end)

      {pid, _name} = start_partition(batch_timeout: 20)

      assert :ok = Partition.append(pid, segment(), 10, 1, wait_until_committed: true)
      assert_receive {:put, _body}, 500
    end

    test "fails if the eventual commit fails, unlike the default" do
      stub(StorageMod, :put, fn _b, _k, _body, _opts -> {:error, :timeout} end)
      Application.put_env(:logflare, :spool, max_commit_attempts: 1)
      on_exit(fn -> Application.delete_env(:logflare, :spool) end)

      {pid, _name} = start_partition(batch_timeout: 20)

      assert {:error, :timeout} =
               Partition.append(pid, segment(), 10, 1, wait_until_committed: true)
    end

    test "multiple concurrent callers landing in the same rolled batch all get replied to individually, in one upload" do
      test_pid = self()

      stub(StorageMod, :put, fn _b, _k, body, _opts ->
        send(test_pid, {:put, body})
        {:ok, %{}}
      end)

      # A short real batch_timeout, rather than manually sending :flush —
      # racing a manual :flush against Task.async's own scheduling could
      # arrive before the append's message even reaches the partition's
      # mailbox. 100ms is comfortably enough for two local GenServer.call
      # sends to land first.
      {pid, _name} = start_partition(batch_timeout: 100)

      task_a =
        Task.async(fn ->
          Partition.append(pid, segment("one\n"), 4, 1, wait_until_committed: true)
        end)

      task_b =
        Task.async(fn ->
          Partition.append(pid, segment("two\n"), 4, 1, wait_until_committed: true)
        end)

      assert Task.await(task_a) == :ok
      assert Task.await(task_b) == :ok

      assert_receive {:put, body}, 1000
      # Order between two concurrently-racing callers isn't guaranteed.
      assert {:ok, segments} = Framing.decode_segments(body)
      assert Enum.sort(segments) == ["one\n", "two\n"]
      refute_receive {:put, _body}, 100
    end
  end

  describe "flush loop" do
    test "a straggler left pending after a big rotation waits for the flush timer, not for its commit slot to free" do
      Application.put_env(:logflare, :spool, mem_max_batch_bytes: 10)
      on_exit(fn -> Application.delete_env(:logflare, :spool) end)
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
      assert :ok = Partition.append(pid, segment("first\n"), 10, 1)
      TestUtils.send_and_wait_for_handling(pid, :noop_sync)
      assert %{task_in_flight: 1} = :sys.get_state(pid)
      assert %{pending_count: 0} = buffer_state(pid)

      assert :ok = Partition.append(pid, segment("second\n"), 5, 1)
      assert %{pending_count: 1} = buffer_state(pid)

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

    test "an oversized append rolls immediately instead of waiting for the timer" do
      Application.put_env(:logflare, :spool, mem_max_batch_bytes: 15)
      on_exit(fn -> Application.delete_env(:logflare, :spool) end)
      test_pid = self()

      stub(StorageMod, :put, fn _b, _k, body, _opts ->
        send(test_pid, {:put, body})
        {:ok, %{}}
      end)

      # Long enough that if the roll below actually waited for this timer,
      # the test would time out instead of passing.
      {pid, _name} = start_partition(batch_timeout: 60_000)

      assert :ok = Partition.append(pid, segment("small\n"), 5, 1)
      assert :ok = Partition.append(pid, segment("big\n"), 10, 1)
      assert_receive {:put, body}, 1000

      assert %{pending_count: 0} = buffer_state(pid)

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
        mem_max_batch_bytes: 4,
        max_inflight_commits: 1,
        recovery_retry_delay_ms: 300
      )

      on_exit(fn -> Application.delete_env(:logflare, :spool) end)
      test_pid = self()

      stub(StorageMod, :put, fn _b, _k, body, _opts ->
        send(test_pid, {:put_started, self(), body})

        receive do
          :proceed -> {:ok, %{}}
        end
      end)

      {pid, _name} = start_partition()

      assert :ok = Partition.append(pid, segment("first\n"), 4, 1)
      assert_receive {:put_started, first_task, first_body}, 1000
      assert %{task_in_flight: 1} = :sys.get_state(pid)

      # Also over budget, but the cap (1) is already saturated. The batch
      # still rolls right away (pending resets to empty) — leaving it
      # un-rolled until a slot freed would let it keep absorbing appends
      # past the byte budget for as long as capacity stayed tight, which is
      # exactly the bug this fixes. Only *starting* this batch's upload is
      # deferred, to the same recovery-style retry a crash-recovered file
      # would use.
      assert :ok = Partition.append(pid, segment("second\n"), 4, 1)
      refute_receive {:put_started, _task, _body}, 100
      assert %{task_in_flight: 1} = :sys.get_state(pid)
      assert %{pending_count: 0} = buffer_state(pid)

      send(first_task, :proceed)

      assert_receive {:put_started, _second_task, second_body}, 1000

      {:ok, [first_payload]} = Framing.decode_segments(first_body)
      {:ok, [second_payload]} = Framing.decode_segments(second_body)
      assert first_payload == "first\n"
      assert second_payload == "second\n"
    end

    test "the buffer never grows past one batch's worth even while capacity stays fully saturated" do
      # recovery_retry_delay_ms doesn't matter for what this test checks —
      # nothing releases the one busy slot, so a deferred rotation's own
      # retry never finds room to spawn regardless of how soon it fires.
      Application.put_env(:logflare, :spool, mem_max_batch_bytes: 4, max_inflight_commits: 1)
      on_exit(fn -> Application.delete_env(:logflare, :spool) end)
      test_pid = self()

      stub(StorageMod, :put, fn _b, _k, body, _opts ->
        send(test_pid, {:put_started, self(), body})

        receive do
          :proceed -> {:ok, %{}}
        end
      end)

      {pid, _name} = start_partition()

      assert :ok = Partition.append(pid, segment("one\n"), 4, 1)
      assert_receive {:put_started, task_a, body_a}, 1000

      # Two more oversized appends while the only commit slot stays busy —
      # each must still roll into its own batch. Before this fix, a
      # saturated cap left the buffer un-rolled, so it would just keep
      # absorbing these past the byte budget instead.
      assert :ok = Partition.append(pid, segment("two\n"), 4, 1)
      assert :ok = Partition.append(pid, segment("three\n"), 4, 1)

      assert %{pending: [], pending_bytes: 0, pending_count: 0} = buffer_state(pid)

      send(task_a, :proceed)
      assert_receive {:put_started, task_b, body_b}, 1000
      send(task_b, :proceed)
      assert_receive {:put_started, task_c, body_c}, 1000
      send(task_c, :proceed)

      assert {:ok, ["one\n"]} = Framing.decode_segments(body_a)
      assert {:ok, ["two\n"]} = Framing.decode_segments(body_b)
      assert {:ok, ["three\n"]} = Framing.decode_segments(body_c)
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

  describe "settle_commit/3 reports Health, for every buffer alike" do
    setup do
      on_exit(fn -> Health.report_recovery!() end)
      :ok
    end

    test "a successful commit reports recovery" do
      Application.put_env(:logflare, :spool, max_spool_health_failures: 1)
      on_exit(fn -> Application.delete_env(:logflare, :spool) end)
      Health.report_failure!()
      assert Health.healthy?() == false

      stub(StorageMod, :put, fn _b, _k, _body, _opts -> {:ok, %{}} end)

      {pid, _name} = start_partition(batch_timeout: 10)
      assert :ok = Partition.append(pid, segment(), 10, 1)

      TestUtils.retry_assert(fn -> assert Health.healthy?() == true end)
    end

    test "a commit that exhausts its retries reports failure" do
      Application.put_env(:logflare, :spool, max_spool_health_failures: 1, max_commit_attempts: 1)
      on_exit(fn -> Application.delete_env(:logflare, :spool) end)

      stub(StorageMod, :put, fn _b, _k, _body, _opts -> {:error, :timeout} end)

      {pid, _name} = start_partition(batch_timeout: 10)
      assert :ok = Partition.append(pid, segment(), 10, 1)

      TestUtils.retry_assert(fn -> assert Health.healthy?() == false end)
    end
  end

  describe "commit task crash resilience" do
    test "a commit task that crashes does not crash the partition" do
      Application.put_env(:logflare, :spool, mem_max_batch_bytes: 10)
      on_exit(fn -> Application.delete_env(:logflare, :spool) end)
      test_pid = self()

      stub(StorageMod, :put, fn _b, _k, _body, _opts ->
        send(test_pid, :put_called)
        raise "boom"
      end)

      {pid, _name} = start_partition(batch_timeout: 60_000)
      ref = Process.monitor(pid)

      assert :ok = Partition.append(pid, segment(), 10, 1)
      assert_receive :put_called, 1000

      refute_receive {:DOWN, ^ref, :process, ^pid, _reason}, 200

      TestUtils.retry_assert(fn -> assert %{task_in_flight: 0} = :sys.get_state(pid) end)
    end
  end

  # Backed by Buffer.WAL, not the rest of this file's Buffer.Mem — see this
  # module's doc for why.
  describe "crash recovery" do
    test "leftover sealed segments from a prior crash are uploaded on the next init" do
      test_pid = self()
      dir = wal_dir!()

      stub(StorageMod, :put, fn _b, _k, body, _opts ->
        send(test_pid, {:put, body})
        {:ok, %{}}
      end)

      # Simulates a sealed segment left behind by a process that died between
      # roll/2 sealing it and a commit task deleting it — no live Partition
      # wrote this file in this test, matching a real crash's on-disk
      # leftovers.
      leftover = Path.join(dir, "p0-999.sealed")
      File.write!(leftover, segment("recovered\n"))

      {_pid, _name} =
        start_partition(buffer_mod: Buffer.WAL, wal_dir: dir, batch_timeout: 60_000)

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
      # batch_timeout — see handle_info({:recover, ...})), not immediately
      # the instant a slot frees.
      {pid, _name} = start_partition(buffer_mod: Buffer.WAL, wal_dir: dir)

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
  end
end
