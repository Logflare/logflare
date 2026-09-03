defmodule Logflare.Backends.Spool.PartitionTest do
  use ExUnit.Case, async: false

  import Mimic

  alias Logflare.Backends.Spool.Framing
  alias Logflare.Backends.Spool.MemoryMonitor
  alias Logflare.Backends.Spool.Partition
  alias Logflare.Backends.Spool.Queue.PubSub, as: QueueMod
  alias Logflare.Backends.Spool.Storage.GCS, as: StorageMod
  alias Logflare.TestUtils

  setup :set_mimic_global

  defp not_throttled! do
    Application.put_env(:logflare, :spool,
      spool_memory_limit_percent: 1.0,
      spool_max_ets_percent: 1.0
    )

    start_supervised!(MemoryMonitor)
    :sys.get_state(MemoryMonitor)
  end

  defp throttled! do
    Application.put_env(:logflare, :spool,
      spool_memory_limit_percent: 0.0,
      spool_max_ets_percent: 0.0
    )

    start_supervised!(MemoryMonitor)
    :sys.get_state(MemoryMonitor)
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
      queue_ref: nil
    ]

    opts = Keyword.merge(defaults, opts)
    pid = start_supervised!({Partition, opts}, id: opts[:name])
    {pid, opts[:name]}
  end

  defp segment(payload \\ "line\n"), do: Framing.encode_segment(payload)

  describe "flush thresholds" do
    test "does not commit while under the byte/count budget" do
      not_throttled!()
      stub(StorageMod, :put, fn _b, _k, _body, _opts -> {:ok, %{}} end)

      {pid, _name} = start_partition(batch_timeout: 60_000)

      task = Task.async(fn -> Partition.append(pid, segment(), 10, 1) end)
      refute_receive({:DOWN, _, _, _, _}, 50)
      assert %{pending_count: 1, pending_bytes: 10} = :sys.get_state(pid)
      Task.shutdown(task, :brutal_kill)
    end

    test "commits immediately once the event-count budget is hit" do
      not_throttled!()
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

    test "commits immediately once the byte budget is hit" do
      not_throttled!()
      test_pid = self()

      stub(StorageMod, :put, fn _b, _k, body, _opts ->
        send(test_pid, {:put, body})
        {:ok, %{}}
      end)

      {pid, _name} = start_partition(batch_timeout: 60_000)

      # 7MB is the hardcoded max_batch_bytes.
      assert :ok = Partition.append(pid, segment(), 7 * 1024 * 1024, 1)
      assert_receive {:put, _body}, 1000
    end

    test "commits at the smaller early-flush budget when throttled" do
      throttled!()
      test_pid = self()

      stub(StorageMod, :put, fn _b, _k, body, _opts ->
        send(test_pid, {:put, body})
        {:ok, %{}}
      end)

      {pid, _name} = start_partition(batch_timeout: 60_000)

      # 3MB alone wouldn't trip the normal 7MB budget, but does trip the
      # throttled 3MB one.
      assert :ok = Partition.append(pid, segment(), 3 * 1024 * 1024, 1)
      assert_receive {:put, _body}, 1000
    end

    test "commits on batch_timeout even without hitting a budget" do
      not_throttled!()
      test_pid = self()

      stub(StorageMod, :put, fn _b, _k, body, _opts ->
        send(test_pid, {:put, body})
        {:ok, %{}}
      end)

      {pid, _name} = start_partition(batch_timeout: 20)

      task = Task.async(fn -> Partition.append(pid, segment(), 10, 1) end)
      assert_receive {:put, _body}, 500
      assert :ok = Task.await(task)
    end
  end

  describe "append/5 vs append_async/4" do
    test "append/5 blocks until the commit replies" do
      not_throttled!()
      stub(StorageMod, :put, fn _b, _k, _body, _opts -> {:ok, %{}} end)

      {pid, _name} = start_partition(batch_timeout: 10)

      assert :ok = Partition.append(pid, segment(), 10, 1)
    end

    test "append_async/4 returns immediately and still gets committed" do
      not_throttled!()
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

  describe "pipelining" do
    test "appends made while a commit is in flight are committed as soon as it finishes, not after a fresh timeout" do
      not_throttled!()
      test_pid = self()

      stub(StorageMod, :put, fn _b, _k, body, _opts ->
        # Slow enough that we can reliably append a second entry while the
        # first commit is still in flight.
        Process.sleep(100)
        send(test_pid, {:put, body})
        {:ok, %{}}
      end)

      # batch_timeout is set far longer than this test's whole run, so the
      # second commit below can only happen via the :commit_done pipelining
      # path (handoff/2 called from handle_info(:commit_done, ...)), never
      # from a fresh timer.
      {pid, _name} = start_partition(batch_timeout: 60_000)

      # Byte-threshold-triggering: hands off immediately, moving straight to
      # in_flight, so the second append below genuinely lands while the first
      # commit is still running.
      Partition.append_async(pid, segment("first\n"), 7 * 1024 * 1024, 1)
      TestUtils.send_and_wait_for_handling(pid, :noop_sync)
      assert %{in_flight: 1, pending: []} = :sys.get_state(pid)

      Partition.append_async(pid, segment("second\n"), 10, 1)
      assert %{in_flight: 1, pending_count: 1} = :sys.get_state(pid)

      assert_receive {:put, first_body}, 1000
      assert_receive {:put, second_body}, 1000

      {:ok, [first_payload]} = Framing.decode_segments(first_body)
      {:ok, [second_payload]} = Framing.decode_segments(second_body)
      assert first_payload == "first\n"
      assert second_payload == "second\n"
    end
  end

  describe "retry on commit failure" do
    test "retries up to max_retries then replies with the final error" do
      not_throttled!()
      Application.put_env(:logflare, :spool, max_retries: 2)
      test_pid = self()

      stub(StorageMod, :put, fn _b, _k, _body, _opts ->
        send(test_pid, :put_attempt)
        {:error, :timeout}
      end)

      {pid, _name} = start_partition(batch_timeout: 10)

      assert {:error, :timeout} = Partition.append(pid, segment(), 10, 1, 5_000)
      assert_receive :put_attempt
      assert_receive :put_attempt
      assert_receive :put_attempt
      refute_receive :put_attempt, 100
    end
  end
end
