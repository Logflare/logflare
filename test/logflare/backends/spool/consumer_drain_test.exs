defmodule Logflare.Backends.Spool.ConsumerDrainTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import Mimic

  alias Logflare.Backends.Spool.ConsumerDrain
  alias Logflare.Backends.Spool.ConsumerPipeline.QueueProducer
  alias Logflare.Backends.Spool.Queue.PubSub, as: QueueMod
  alias Logflare.Backends.Spool.SpoolAck
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

    :ok
  end

  defp run(opts) do
    ConsumerDrain.run(Keyword.merge([timeout_ms: 1_000, poll_interval_ms: 10], opts))
  end

  defp stub_producers(drained?) do
    test_pid = self()

    stub(QueueProducer, :stop_fetching, fn producer ->
      send(test_pid, {:stop_fetching, producer})
      :ok
    end)

    stub(QueueProducer, :drained?, fn _producer -> drained?.() end)
  end

  test "stops every producer fetching and returns :drained once nothing is pending" do
    stub_producers(fn -> true end)
    TestUtils.attach_forwarder([:logflare, :backends, :spool, :consumer, :drain])

    assert run(producers: [:p0, :p1]) == :drained

    assert_received {:stop_fetching, :p0}
    assert_received {:stop_fetching, :p1}
    refute_received {:nacked, _}

    assert_receive {:telemetry_event, [:logflare, :backends, :spool, :consumer, :drain],
                    %{duration: _, pending: 0}, %{result: :drained}}
  end

  test "waits for in-flight files to be fully processed and acked" do
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

  test "waits for a producer that still has an unemitted file" do
    {:ok, drained} = Agent.start_link(fn -> false end)
    stub_producers(fn -> Agent.get(drained, & &1) end)

    task = Task.async(fn -> run(producers: [:p0]) end)

    refute Task.yield(task, 100)

    Agent.update(drained, fn _ -> true end)

    assert Task.await(task) == :drained
  end

  test "nacks whatever is still pending at the deadline so it redelivers immediately" do
    stub_producers(fn -> true end)
    TestUtils.attach_forwarder([:logflare, :backends, :spool, :consumer, :drain])
    SpoolAck.register("stuck", QueueMod, "queue-url")
    SpoolAck.bump("stuck", 1)

    started = System.monotonic_time(:millisecond)

    assert run(producers: [:p0], timeout_ms: 200) == :timeout

    assert System.monotonic_time(:millisecond) - started >= 200
    assert_received {:nacked, "stuck"}
    assert SpoolAck.pending_count() == 0

    assert_receive {:telemetry_event, [:logflare, :backends, :spool, :consumer, :drain],
                    %{pending: 1}, %{result: :timeout}}
  end

  test "treats a producer that is already gone as drained" do
    dead = spawn(fn -> :ok end)
    ref = Process.monitor(dead)
    assert_receive {:DOWN, ^ref, :process, ^dead, _}

    assert run(producers: [dead]) == :drained
  end

  test "drains straight away when no consumer pipeline is running" do
    assert ConsumerDrain.run(timeout_ms: 100) == :drained
  end

  describe "verification logging" do
    test "logs when the drain starts and how many producers stopped fetching" do
      stub_producers(fn -> true end)

      log = capture_log([level: :warning], fn -> run(producers: [:p0, :p1]) end)

      assert log =~ "spool_consumer: drain started, stopped fetching on 2 producer(s)"
      assert log =~ "waiting up to 1000ms"
    end

    test "logs a drained result with nothing left pending" do
      stub_producers(fn -> true end)

      log = capture_log([level: :warning], fn -> run(producers: [:p0]) end)

      assert log =~ ~r/spool_consumer: drain finished result=drained pending=0 duration_ms=\d+/
    end

    test "logs a timed out result with how many files were nacked" do
      stub_producers(fn -> true end)
      SpoolAck.register("stuck", QueueMod, "queue-url")
      SpoolAck.bump("stuck", 1)

      log = capture_log([level: :warning], fn -> run(producers: [:p0], timeout_ms: 50) end)

      assert log =~ ~r/spool_consumer: drain finished result=timeout pending=1 duration_ms=\d+/
    end
  end
end
