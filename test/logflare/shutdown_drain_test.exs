defmodule Logflare.ShutdownDrainTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import Mimic

  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.Spool.ConsumerPipeline.QueueProducer
  alias Logflare.Backends.Spool.Queue.PubSub, as: QueueMod
  alias Logflare.Backends.Spool.SpoolAck
  alias Logflare.ShutdownDrain
  alias Logflare.TestUtils

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

  defp stub_producers(drained?) do
    test_pid = self()

    stub(QueueProducer, :stop_fetching, fn producer ->
      send(test_pid, {:stop_fetching, producer})
      :ok
    end)

    stub(QueueProducer, :drained?, fn _producer -> drained?.() end)
  end

  defp stub_queued_events(initial) do
    {:ok, queued} = Agent.start_link(fn -> initial end)
    stub(IngestEventQueue, :total_size, fn -> Agent.get(queued, & &1) end)
    queued
  end

  test "stops every spool producer fetching and returns :drained once nothing is pending" do
    stub_producers(fn -> true end)
    TestUtils.attach_forwarder([:logflare, :shutdown, :drain])

    assert run(producers: [:p0, :p1]) == :drained

    assert_received {:stop_fetching, :p0}
    assert_received {:stop_fetching, :p1}
    refute_received {:nacked, _}

    assert_receive {:telemetry_event, [:logflare, :shutdown, :drain],
                    %{duration: _, spool_pending: 0, queued_events: 0}, %{result: :drained}}
  end

  test "waits for in-flight spool files to be fully processed and acked" do
    stub_producers(fn -> true end)
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

  test "waits for a spool producer that still has an unemitted file" do
    {:ok, drained} = Agent.start_link(fn -> false end)
    stub_producers(fn -> Agent.get(drained, & &1) end)

    task = Task.async(fn -> run(producers: [:p0]) end)

    refute Task.yield(task, 100)

    Agent.update(drained, fn _ -> true end)

    assert Task.await(task) == :drained
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
    stub_producers(fn -> true end)
    TestUtils.attach_forwarder([:logflare, :shutdown, :drain])
    SpoolAck.register("stuck", QueueMod, "queue-url")
    SpoolAck.bump("stuck", 1)

    started = System.monotonic_time(:millisecond)

    assert run(producers: [:p0], timeout_ms: 200) == :timeout

    assert System.monotonic_time(:millisecond) - started >= 200
    assert_received {:nacked, "stuck"}
    assert SpoolAck.pending_count() == 0

    assert_receive {:telemetry_event, [:logflare, :shutdown, :drain], %{spool_pending: 1},
                    %{result: :timeout}}
  end

  test "times out on queued events that never drain" do
    stub_queued_events(7)
    TestUtils.attach_forwarder([:logflare, :shutdown, :drain])

    assert run(producers: [], timeout_ms: 100) == :timeout

    assert_receive {:telemetry_event, [:logflare, :shutdown, :drain], %{queued_events: 7},
                    %{result: :timeout}}
  end

  test "treats a spool producer that is already gone as drained" do
    dead = spawn(fn -> :ok end)
    ref = Process.monitor(dead)
    assert_receive {:DOWN, ^ref, :process, ^dead, _}

    assert run(producers: [dead]) == :drained
  end

  test "drains straight away when no spool consumer pipeline is running" do
    assert ShutdownDrain.run(timeout_ms: 100, min_wait_ms: 0) == :drained
  end

  describe "verification logging" do
    test "logs when the drain starts and how many spool producers stopped fetching" do
      stub_producers(fn -> true end)

      log = capture_log([level: :warning], fn -> run(producers: [:p0, :p1], min_wait_ms: 10) end)

      assert log =~ "shutdown_drain: started, stopped fetching on 2 spool producer(s)"
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
