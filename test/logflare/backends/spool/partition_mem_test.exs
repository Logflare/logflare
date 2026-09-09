defmodule Logflare.Backends.Spool.PartitionMemTest do
  use ExUnit.Case, async: false

  import Mimic

  alias Logflare.Backends.Spool.Buffer
  alias Logflare.Backends.Spool.Framing
  alias Logflare.Backends.Spool.Partition
  alias Logflare.Backends.Spool.Queue.PubSub, as: QueueMod
  alias Logflare.Backends.Spool.Storage.GCS, as: StorageMod

  setup :set_mimic_global

  defp start_partition(opts) do
    defaults = [
      name: :"partition_mem_#{System.unique_integer([:positive])}",
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

  defp segment(payload \\ "line\n"), do: Framing.encode_segment(payload)

  describe "append/5 vs append_committed/5" do
    test "append/5 replies as soon as the segment is admitted to the buffer, without waiting for a commit" do
      test_pid = self()

      stub(StorageMod, :put, fn _b, _k, _body, _opts ->
        send(test_pid, :put_called)
        Process.sleep(:infinity)
      end)

      {pid, _name} = start_partition(batch_timeout: 60_000)

      assert :ok = Partition.append(pid, segment(), 10, 1)
      refute_receive :put_called, 50
    end

    test "append_committed/5 blocks until the batch is actually committed" do
      test_pid = self()

      stub(StorageMod, :put, fn _b, _k, body, _opts ->
        send(test_pid, {:put, body})
        {:ok, %{}}
      end)

      {pid, _name} = start_partition(batch_timeout: 20)

      assert :ok = Partition.append_committed(pid, segment(), 10, 1)
      assert_receive {:put, _body}, 500
    end
  end

  describe "group commit" do
    test "multiple concurrent append_committed/5 calls landing in the same batch get committed together, in one upload" do
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

      task_a = Task.async(fn -> Partition.append_committed(pid, segment("one\n"), 4, 1) end)
      task_b = Task.async(fn -> Partition.append_committed(pid, segment("two\n"), 4, 1) end)

      assert Task.await(task_a) == :ok
      assert Task.await(task_b) == :ok

      assert_receive {:put, body}, 1000
      # Order between two concurrently-racing callers isn't guaranteed.
      assert {:ok, segments} = Framing.decode_segments(body)
      assert Enum.sort(segments) == ["one\n", "two\n"]
      refute_receive {:put, _body}, 100
    end

    test "rolls early once the byte threshold is crossed, without waiting for the timer" do
      test_pid = self()

      stub(StorageMod, :put, fn _b, _k, body, _opts ->
        send(test_pid, {:put, body}) && {:ok, %{}}
      end)

      Application.put_env(:logflare, :spool, mem_max_batch_bytes: 5)
      on_exit(fn -> Application.delete_env(:logflare, :spool) end)

      {pid, _name} = start_partition(batch_timeout: 60_000)

      assert :ok = Partition.append_committed(pid, segment("one\n"), 10, 1)
      assert_receive {:put, _body}, 500
    end

    test "unlike the old single-slot design, more than one batch can be in flight at once (bounded by max_inflight_commits, shared with the WAL buffer)" do
      test_pid = self()

      stub(StorageMod, :put, fn _b, _k, body, _opts ->
        send(test_pid, {:put_started, body})
        Process.sleep(100)
        {:ok, %{}}
      end)

      Application.put_env(:logflare, :spool, mem_max_batch_bytes: 4)
      on_exit(fn -> Application.delete_env(:logflare, :spool) end)

      {pid, _name} = start_partition(batch_timeout: 60_000)

      task_a = Task.async(fn -> Partition.append_committed(pid, segment("first\n"), 4, 1) end)
      assert_receive {:put_started, _first_body}, 500

      # A second batch, while the first is still uploading — starts
      # immediately rather than waiting, since the default
      # max_inflight_commits (10) comfortably covers two.
      task_b = Task.async(fn -> Partition.append_committed(pid, segment("second\n"), 4, 1) end)
      assert_receive {:put_started, _second_body}, 200

      assert Task.await(task_a) == :ok
      assert Task.await(task_b) == :ok
    end
  end

  describe "commit failure" do
    test "retries are governed by the same max_commit_attempts/retry_delay_ms as the WAL buffer, then fail every blocked caller" do
      test_pid = self()
      {:ok, counter} = Agent.start_link(fn -> 0 end)

      stub(StorageMod, :put, fn _b, _k, _body, _opts ->
        attempt = Agent.get_and_update(counter, &{&1, &1 + 1})
        send(test_pid, {:put_attempt, attempt})
        {:error, :timeout}
      end)

      Application.put_env(:logflare, :spool, max_commit_attempts: 2, retry_delay_ms: 1)
      on_exit(fn -> Application.delete_env(:logflare, :spool) end)

      {pid, _name} = start_partition(batch_timeout: 100)

      task = Task.async(fn -> Partition.append_committed(pid, segment(), 4, 1) end)

      assert_receive {:put_attempt, 0}, 500
      assert_receive {:put_attempt, 1}, 500
      refute_receive {:put_attempt, 2}, 100

      assert Task.await(task) == {:error, :timeout}
    end

    test "a commit that succeeds on retry unblocks every caller with :ok" do
      {:ok, counter} = Agent.start_link(fn -> 0 end)

      stub(StorageMod, :put, fn _b, _k, _body, _opts ->
        attempt = Agent.get_and_update(counter, &{&1, &1 + 1})
        if attempt == 0, do: {:error, :timeout}, else: {:ok, %{}}
      end)

      Application.put_env(:logflare, :spool, max_commit_attempts: 2, retry_delay_ms: 1)
      on_exit(fn -> Application.delete_env(:logflare, :spool) end)

      {pid, _name} = start_partition(batch_timeout: 100)

      task = Task.async(fn -> Partition.append_committed(pid, segment(), 4, 1) end)

      assert Task.await(task) == :ok
    end

    test "a crashing commit task fails every blocked caller in that batch instead of leaving them hanging forever" do
      stub(StorageMod, :put, fn _b, _k, _body, _opts -> raise "boom" end)

      {pid, _name} = start_partition(batch_timeout: 100)

      task = Task.async(fn -> Partition.append_committed(pid, segment(), 4, 1) end)

      assert {:error, _reason} = Task.await(task)
    end
  end
end
