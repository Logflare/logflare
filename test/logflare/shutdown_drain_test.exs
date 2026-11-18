defmodule Logflare.ShutdownDrainTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import Mimic

  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.Spool.ConsumerPipeline.QueueProducer
  alias Logflare.Backends.Spool.DurableBuffer.Backends.Cloud
  alias Logflare.Backends.Spool.DurableBuffer.Backends.RotatingWal
  alias Logflare.Backends.Spool.Health
  alias Logflare.Backends.Spool.MemoryMonitor
  alias Logflare.Backends.Spool.ObjectDeleter
  alias Logflare.Backends.Spool.Queue.PubSub, as: QueueMod
  alias Logflare.Backends.Spool.SpoolAck
  alias Logflare.Backends.Spool.Storage.GCS, as: StorageMod
  alias Logflare.Shutdown.Budget
  alias Logflare.ShutdownDrain

  setup :set_mimic_global

  setup do
    :ets.delete_all_objects(:spool_ack)
    on_exit(fn -> :ets.delete_all_objects(:spool_ack) end)

    test_pid = self()

    stub(QueueMod, :ack, fn _url, h ->
      send(test_pid, {:acked, h})
      :ok
    end)

    stub(QueueMod, :nack, fn _url, h ->
      send(test_pid, {:nacked, h})
      :ok
    end)

    stub(IngestEventQueue, :total_size, fn -> 0 end)

    :ok
  end

  defp run(opts) do
    now = System.monotonic_time(:millisecond)
    queue_ms = Keyword.get(opts, :timeout_ms, 1_000)
    sync_ms = Keyword.get(opts, :spool_sync_timeout_ms, 10_000)
    cleanup_ms = Keyword.get(opts, :cleanup_timeout_ms, 10_000)
    queues_end = min(Keyword.get(opts, :deadline_ms, now + queue_ms), now + queue_ms)

    budget = %{
      queues: {queues_end, queue_ms},
      sync: {queues_end + sync_ms, sync_ms},
      cleanup: {queues_end + sync_ms + cleanup_ms, cleanup_ms}
    }

    producers = Keyword.get(opts, :producers, [])

    intake_result =
      Keyword.get_lazy(opts, :intake_result, fn ->
        ShutdownDrain.stop_receiving(Budget.deadline(budget.queues), producers)
      end)

    ShutdownDrain.run(budget, intake_result, opts[:spool_buffer],
      poll_interval_ms: 10,
      producers: Keyword.get(opts, :live_producers, fn -> producers end)
    )
  end

  defp stub_producers do
    test_pid = self()

    stub(QueueProducer, :stop_receiving, fn producer, _timeout ->
      send(test_pid, {:stop_receiving, producer})
      :ok
    end)

    stub(QueueProducer, :drained?, fn _producer, _timeout -> true end)
  end

  defp stub_queued_events(initial) do
    {:ok, queued} = Agent.start_link(fn -> initial end)
    stub(IngestEventQueue, :total_size, fn -> Agent.get(queued, & &1) end)
    queued
  end

  test "stops every spool producer receiving and returns :drained once nothing is pending" do
    stub_producers()

    assert run(producers: [:p0, :p1]) == :drained

    assert_received {:stop_receiving, :p0}
    assert_received {:stop_receiving, :p1}
    refute_received {:nacked, _}
  end

  test "waits for in-flight spool files to be fully processed and acked" do
    stub_producers()
    handle = "h1"
    SpoolAck.register(handle, QueueMod, "queue-url")
    SpoolAck.bump(handle, 1)

    task = Task.async(fn -> run(producers: [:p0]) end)

    refute Task.yield(task, 100)

    SpoolAck.ack(handle, 1)

    assert Task.await(task) == :drained
    assert_received {:acked, ^handle}
    refute_received {:nacked, ^handle}
  end

  test "waits for events queued for backends or the spool to be delivered" do
    queued = stub_queued_events(3)

    task = Task.async(fn -> run(producers: []) end)

    refute Task.yield(task, 100)

    Agent.update(queued, fn _ -> 0 end)

    assert Task.await(task) == :drained
  end

  test "does not resend stop requests to producers that already received them" do
    stub_producers()

    assert run(producers: [:p0, :p1]) == :drained
    assert_received {:stop_receiving, :p0}
    assert_received {:stop_receiving, :p1}
    refute_received {:stop_receiving, _}
  end

  test "checks a replacement producer without sending another stop request" do
    stub_producers()
    parent = self()

    stub(QueueProducer, :drained?, fn producer, _timeout ->
      send(parent, {:checked, producer})
      true
    end)

    intake_result =
      ShutdownDrain.stop_receiving(System.monotonic_time(:millisecond) + 1_000, [:original])

    assert run(intake_result: intake_result, live_producers: fn -> [:replacement] end) == :drained
    assert_received {:stop_receiving, :original}
    assert_received {:checked, :replacement}
    refute_received {:stop_receiving, _}
    refute_received {:checked, :original}
  end

  test "a live but unresponsive producer is not treated as drained" do
    {:ok, producer} = Agent.start_link(fn -> nil end)
    :ok = :sys.suspend(producer)

    assert run(producers: [producer], timeout_ms: 50) == :timeout
    assert Process.alive?(producer)
    :sys.resume(producer)
  end

  test "nacks spool files still pending at the deadline so they redeliver immediately" do
    stub_producers()
    SpoolAck.register("stuck", QueueMod, "queue-url")
    SpoolAck.bump("stuck", 1)

    started = System.monotonic_time(:millisecond)

    assert run(producers: [:p0], timeout_ms: 200) == :timeout

    assert System.monotonic_time(:millisecond) - started >= 200
    assert_received {:nacked, "stuck"}
    assert SpoolAck.pending_count() == 0
  end

  test "times out on queued events that never drain" do
    stub_queued_events(7)

    assert run(producers: [], timeout_ms: 100) == :timeout
  end

  test "the shared deadline bounds queue waiting instead of adding another full phase budget" do
    stub_queued_events(7)
    started = System.monotonic_time(:millisecond)

    assert run(
             producers: [],
             timeout_ms: 1_000,
             deadline_ms: started + 50,
             spool_sync_timeout_ms: 0,
             cleanup_timeout_ms: 0
           ) == :timeout

    assert System.monotonic_time(:millisecond) - started < 500
  end

  test "acknowledgment flush timeouts are not reported as drained" do
    stub(SpoolAck, :flush, fn _timeout -> :timeout end)
    assert run(producers: []) == :timeout
  end

  test "skips a spool producer that is already gone" do
    dead = spawn(fn -> :ok end)
    ref = Process.monitor(dead)
    assert_receive {:DOWN, ^ref, :process, ^dead, _}

    assert run(producers: [dead]) == :drained
  end

  test "drains straight away when no spool consumer pipeline is running" do
    intake_result = ShutdownDrain.stop_receiving(System.monotonic_time(:millisecond) + 100)
    assert intake_result == :ok
    assert run(intake_result: intake_result, timeout_ms: 100) == :drained
  end

  test "preserves the intake timeout when no final drain is configured" do
    assert ShutdownDrain.run(nil, :timeout, nil) == :timeout
  end

  test "a blocked nack at the deadline cannot hold the drain past its cleanup budget" do
    stub(QueueMod, :nack, fn _url, _h -> Process.sleep(:infinity) end)
    SpoolAck.register("stuck", QueueMod, "queue-url")
    SpoolAck.bump("stuck", 1)

    {elapsed_us, result} =
      :timer.tc(fn -> run(producers: [], timeout_ms: 50, cleanup_timeout_ms: 200) end)

    assert result == :timeout
    assert elapsed_us < 1_000_000
  end

  describe "object deletes" do
    setup do
      assert ObjectDeleter.flush(10_000) == :ok
      :ok
    end

    test "waits for spool object deletes still buffered after the final acks" do
      test_pid = self()

      stub(StorageMod, :delete, fn _bucket, key ->
        Process.sleep(100)
        send(test_pid, {:deleted, key})
        :ok
      end)

      SpoolAck.register("h1", QueueMod, "queue-url", {StorageMod, "bucket", "0/a.etf"})
      SpoolAck.bump("h1", 1)
      SpoolAck.ack("h1", 1)

      assert run(producers: []) == :drained

      assert_received {:deleted, "0/a.etf"}
    end

    test "gives up on object deletes at the cleanup budget instead of holding the drain" do
      stub(StorageMod, :delete, fn _bucket, _key ->
        Process.sleep(400)
        :ok
      end)

      ObjectDeleter.enqueue({StorageMod, "bucket", "0/slow.etf"})

      started = System.monotonic_time(:millisecond)
      log = capture_log([level: :warning], fn -> run(producers: [], cleanup_timeout_ms: 50) end)

      assert System.monotonic_time(:millisecond) - started < 350
      assert log =~ "object_deletes=timeout"

      assert ObjectDeleter.flush(2_000) == :ok
    end
  end

  describe "spool consumer prefetch" do
    setup do
      original = Application.get_env(:logflare, :spool)

      on_exit(fn ->
        if original,
          do: Application.put_env(:logflare, :spool, original),
          else: Application.delete_env(:logflare, :spool)
      end)

      Application.put_env(:logflare, :spool,
        spool_memory_limit_percent: 1.0,
        spool_max_ets_percent: 1.0
      )

      start_supervised!(MemoryMonitor)
      :sys.get_state(MemoryMonitor)
      :ok
    end

    test "waits for a prefetch still downloading, so its queue receipt is nacked before shutdown" do
      test_pid = self()

      {:ok, messages} =
        Agent.start_link(fn -> [%{id: "h1", body: ~s({"file_key":"0/a.v3.etf"})}] end)

      stub(QueueMod, :receive, fn _url, _opts ->
        {:ok, Agent.get_and_update(messages, &{&1, []})}
      end)

      stub(StorageMod, :get, fn _bucket, _key ->
        send(test_pid, {:download_started, self()})

        receive do
          :proceed -> {:error, :timeout}
        end
      end)

      producer =
        start_supervised!(
          {QueueProducer,
           queue_url: "q", bucket: "test-bucket", storage_mod: StorageMod, queue_mod: QueueMod}
        )

      :sys.replace_state(producer, fn stage -> put_in(stage.state.demand, 1) end)
      send(producer, :poll)
      assert_receive {:download_started, downloader}, 2_000

      task = Task.async(fn -> run(producers: [producer], timeout_ms: 5_000) end)

      refute Task.yield(task, 300)

      send(downloader, :proceed)

      assert Task.await(task) == :drained
      assert_received {:nacked, "h1"}
    end
  end

  describe "spool producer buffer" do
    setup do
      on_exit(fn -> Health.report_recovery!(:upload) end)
    end

    defp start_spool_buffer!(wal_dir \\ nil) do
      name = :"shutdown_drain_buffer_#{System.unique_integer([:positive])}"

      cloud =
        {Cloud,
         bucket: "test-bucket",
         storage_mod: StorageMod,
         queue_mod: QueueMod,
         queue_ref: nil,
         compress: false,
         max_commit_attempts: 3,
         retry_delay_ms: 1}

      backend =
        if wal_dir do
          {RotatingWal,
           wal_dir: wal_dir, inner_backend: cloud, worker_count: 1, max_batch_bytes: 1}
        else
          cloud
        end

      start_supervised!({DurableBuffer, name: name, partitions: 1, backend: backend})
      name
    end

    defp block_uploads do
      test_pid = self()

      stub(StorageMod, :put, fn _bucket, _key, body, _opts ->
        send(test_pid, {:upload_started, self()})

        receive do
          :release ->
            send(test_pid, {:uploaded, body})
            {:ok, %{}}
        end
      end)
    end

    defp append_event!(buffer) do
      :ok = DurableBuffer.append_async(buffer, :key, <<1::32-big, "event">>)
      assert_receive {:upload_started, uploader}, 2_000
      uploader
    end

    test "waits for events accepted into the spool buffer to be uploaded" do
      block_uploads()
      buffer = start_spool_buffer!()
      uploader = append_event!(buffer)

      task = Task.async(fn -> run(producers: [], spool_buffer: buffer) end)

      refute Task.yield(task, 300)

      send(uploader, :release)

      assert Task.await(task) == :drained
      assert_received {:uploaded, _body}
    end

    @tag :tmp_dir
    test "reports local WAL sync without claiming the blocked upload completed", %{tmp_dir: dir} do
      block_uploads()
      buffer = start_spool_buffer!(dir)
      uploader = append_event!(buffer)

      try do
        log =
          capture_log([level: :warning], fn ->
            assert run(producers: [], spool_buffer: buffer) == :drained
          end)

        assert log =~ "spool_buffer=local_wal_synced_cloud_delivery_unverified"
        refute_received {:uploaded, _}
      after
        send(uploader, :release)
      end

      assert_receive {:uploaded, _}
    end

    test "gives up on the spool buffer once its sync timeout passes" do
      block_uploads()
      buffer = start_spool_buffer!()
      uploader = append_event!(buffer)

      {elapsed_us, result} =
        :timer.tc(fn ->
          run(producers: [], spool_buffer: buffer, spool_sync_timeout_ms: 200)
        end)

      assert result == :timeout
      assert elapsed_us < 1_000_000

      send(uploader, :release)
    end

    test "logs the effective spool sync limit" do
      stub(StorageMod, :put, fn _bucket, _key, _body, _opts -> {:ok, %{}} end)
      buffer = start_spool_buffer!()
      :ok = DurableBuffer.append_async(buffer, :key, <<1::32-big, "event">>)

      log =
        capture_log([level: :warning], fn ->
          assert run(producers: [], spool_buffer: buffer) == :drained
        end)

      assert log =~ ~r/shutdown_drain: syncing the spool buffer for up to (999\d|10000)ms/
    end
  end

  describe "verification logging" do
    test "logs how many spool producers are asked to stop receiving" do
      stub_producers()

      log = capture_log([level: :warning], fn -> run(producers: [:p0, :p1]) end)

      assert log =~ "shutdown_drain: requesting intake stop on 2 spool producer(s)"
    end

    test "logs a drained result with nothing left pending" do
      log = capture_log([level: :warning], fn -> run(producers: []) end)

      assert log =~
               ~r/shutdown_drain: finished result=drained spool_pending=0 queued_events=0 duration_ms=\d+/
    end

    test "logs a timed out result with what was left behind" do
      stub_queued_events(4)
      SpoolAck.register("stuck", QueueMod, "queue-url")
      SpoolAck.bump("stuck", 1)

      log = capture_log([level: :warning], fn -> run(producers: [], timeout_ms: 50) end)

      assert log =~
               ~r/shutdown_drain: finished result=timeout spool_pending=1 queued_events=4 duration_ms=\d+/
    end
  end
end
