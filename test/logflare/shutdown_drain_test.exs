defmodule Logflare.ShutdownDrainTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import Mimic

  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.Spool.ConsumerPipeline.QueueProducer
  alias Logflare.Backends.Spool.DurableBuffer.Backends.Cloud
  alias Logflare.Backends.Spool.Health
  alias Logflare.Backends.Spool.MemoryMonitor
  alias Logflare.Backends.Spool.Queue.PubSub, as: QueueMod
  alias Logflare.Backends.Spool.SpoolAck
  alias Logflare.Backends.Spool.Storage.GCS, as: StorageMod
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
    ShutdownDrain.run(
      Keyword.merge([timeout_ms: 1_000, min_wait_ms: 0, poll_interval_ms: 10], opts)
    )
  end

  defp stub_producers do
    test_pid = self()

    stub(QueueProducer, :stop_receiving, fn producer ->
      send(test_pid, {:stop_receiving, producer})
      :ok
    end)

    stub(QueueProducer, :drained?, fn _producer -> true end)
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

  test "never finishes before the minimum wait, so HTTP clients can stop sending first" do
    started = System.monotonic_time(:millisecond)

    assert run(producers: [], min_wait_ms: 200) == :drained

    assert System.monotonic_time(:millisecond) - started >= 200
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

  test "skips a spool producer that is already gone" do
    dead = spawn(fn -> :ok end)
    ref = Process.monitor(dead)
    assert_receive {:DOWN, ^ref, :process, ^dead, _}

    assert run(producers: [dead]) == :drained
  end

  test "drains straight away when no spool consumer pipeline is running" do
    assert ShutdownDrain.run(timeout_ms: 100, min_wait_ms: 0) == :drained
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

    defp start_spool_buffer! do
      name = :"shutdown_drain_buffer_#{System.unique_integer([:positive])}"

      start_supervised!(
        {DurableBuffer,
         name: name,
         partitions: 1,
         backend:
           {Cloud,
            bucket: "test-bucket",
            storage_mod: StorageMod,
            queue_mod: QueueMod,
            queue_ref: nil,
            compress: false,
            max_commit_attempts: 3,
            retry_delay_ms: 1}}
      )

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

    test "syncs the spool buffer for up to 10s by default" do
      stub(StorageMod, :put, fn _bucket, _key, _body, _opts -> {:ok, %{}} end)
      buffer = start_spool_buffer!()
      :ok = DurableBuffer.append_async(buffer, :key, <<1::32-big, "event">>)

      log =
        capture_log([level: :warning], fn ->
          assert run(producers: [], spool_buffer: buffer) == :drained
        end)

      assert log =~ "shutdown_drain: syncing the spool buffer for up to 10000ms"
    end
  end

  describe "verification logging" do
    test "logs when the drain starts and how many spool producers stopped receiving" do
      stub_producers()

      log = capture_log([level: :warning], fn -> run(producers: [:p0, :p1], min_wait_ms: 10) end)

      assert log =~ "shutdown_drain: started, stopped receiving on 2 spool producer(s)"
      assert log =~ "waiting at least 10ms and up to 1000ms"
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
