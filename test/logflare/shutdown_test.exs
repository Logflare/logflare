defmodule Logflare.ShutdownTest do
  use ExUnit.Case, async: false

  import Mimic

  alias Logflare.Backends.Spool.DurableBuffer.Backends.Cloud
  alias Logflare.Backends.Spool.DurableBuffer.Supervisor, as: SpoolBuffer
  alias Logflare.Backends.Spool.Queue.PubSub, as: QueueMod
  alias Logflare.Backends.Spool.Storage.GCS
  alias Logflare.Backends.Spool.SpoolAck
  alias Logflare.IngressDrain
  alias Logflare.IngressGate
  alias Logflare.Shutdown
  alias Logflare.Shutdown.Budget
  alias Logflare.ShutdownDrain
  alias Logflare.TestUtils

  setup :set_mimic_global

  setup do
    :ets.insert(IngressGate, {:admission, :open})
    on_exit(fn -> :ets.insert(IngressGate, {:admission, :open}) end)

    parent = self()

    stub(ShutdownDrain, :stop_receiving, fn _deadline ->
      send(parent, {:phase, :stop_receiving})
      :ok
    end)

    stub(IngressDrain, :run, fn deadline, opts ->
      assert opts[:listener_deadline] != nil
      send(parent, {:phase, {:quiesce, deadline}})
      :drained
    end)

    stub(ShutdownDrain, :run, fn
      nil, _, _ ->
        :drained

      flush, _, _ ->
        send(parent, {:phase, {:flush, flush}})
        :drained
    end)

    :ok
  end

  test "withdrawal and ingress completion precede the final flush" do
    deadline = System.monotonic_time(:millisecond) + 1_000

    assert Shutdown.run(
             deadline_ms: deadline,
             withdrawal_ms: 0,
             request_timeout_ms: 100,
             drain_timeout_ms: 500
           ) == :drained

    assert_receive {:phase, first}
    assert_receive {:phase, second}
    assert_receive {:phase, third}
    assert first == :stop_receiving
    assert {:quiesce, ingress_deadline} = second
    assert ingress_deadline <= deadline
    assert {:flush, %{cleanup: {^deadline, _}, queues: {_, 500}}} = third
  end

  test "does not flush while an admitted request is still being awaited" do
    parent = self()

    stub(IngressDrain, :run, fn _deadline, _opts ->
      send(parent, {:waiting, self()})

      receive do
        :release -> :drained
      end
    end)

    task =
      Task.async(fn ->
        Shutdown.run(
          deadline_ms: System.monotonic_time(:millisecond) + 2_000,
          withdrawal_ms: 0,
          request_timeout_ms: 1_000,
          drain_timeout_ms: 500
        )
      end)

    assert_receive {:waiting, worker}
    refute_receive {:phase, {:flush, _}}
    send(worker, :release)
    assert Task.await(task) == :drained
    assert_receive {:phase, {:flush, _}}
  end

  test "an ingress timeout is never reported as a successful drain" do
    stub(IngressDrain, :run, fn _deadline, _opts -> :timeout end)

    assert Shutdown.run(
             deadline_ms: System.monotonic_time(:millisecond) + 1_000,
             withdrawal_ms: 0,
             request_timeout_ms: 10,
             drain_timeout_ms: 500
           ) == :timeout

    assert_receive {:phase, {:flush, _}}
  end

  test "preserves a final spool sync error when ingress drained" do
    stub(ShutdownDrain, :run, fn _budget, _intake_result, _buffer -> :error end)

    assert Shutdown.run(
             deadline_ms: System.monotonic_time(:millisecond) + 1_000,
             withdrawal_ms: 0,
             request_timeout_ms: 10,
             drain_timeout_ms: 500
           ) == :error
  end

  test "reserves sync and flushes a configured spool that starts before admission seals" do
    name = SpoolBuffer.name()
    previous = Application.get_env(:logflare, :spool, [])
    Application.put_env(:logflare, :spool, Keyword.put(previous, :mode, :producer))

    on_exit(fn ->
      Application.put_env(:logflare, :spool, previous)
      :persistent_term.erase({DurableBuffer, name})
    end)

    assert SpoolBuffer.partitions() == []

    stub(IngressDrain, :run, fn _deadline, _opts ->
      start_supervised!(
        {DurableBuffer,
         name: name,
         partitions: 1,
         backend: {Cloud, bucket: "test", storage_mod: GCS, queue_mod: nil, queue_ref: nil}}
      )

      :ok = DurableBuffer.append_async(name, :key, <<1::32-big, "event">>)
      :drained
    end)

    expect(GCS, :put, fn "test", _key, _body, _headers -> {:ok, %{}} end)

    stub(ShutdownDrain, :run, fn flush, :ok, buffer ->
      assert {_, 10_000} = flush.sync
      assert buffer == name
      assert ShutdownDrain.sync_spool_buffer(flush.sync, buffer) == :synced
      :drained
    end)

    assert Shutdown.run(
             deadline_ms: System.monotonic_time(:millisecond) + 4_000,
             withdrawal_ms: 0,
             request_timeout_ms: 100,
             drain_timeout_ms: 500
           ) == :drained
  end

  test "a configured buffer that never starts is not reported as disabled" do
    assert ShutdownDrain.sync_spool_buffer(
             {System.monotonic_time(:millisecond) + 1_000, 1_000},
             :missing_shutdown_spool
           ) == :error
  end

  test "without spool drain configuration, still quiesces ingress before stopping" do
    assert Shutdown.run(
             deadline_ms: System.monotonic_time(:millisecond) + 1_000,
             withdrawal_ms: 0,
             request_timeout_ms: 100,
             drain_timeout_ms: nil
           ) == :drained

    assert_receive {:phase, {:quiesce, _}}
    refute_receive {:phase, {:flush, _}}
  end

  test "withdrawal is bounded by the shared deadline" do
    started = System.monotonic_time(:millisecond)

    Shutdown.run(
      deadline_ms: started + 500,
      withdrawal_ms: 10_000,
      request_timeout_ms: 100,
      drain_timeout_ms: 500
    )

    assert System.monotonic_time(:millisecond) - started < 1_000
    assert_receive {:phase, {:flush, budget}}
    assert Budget.remaining(Budget.deadline(budget.cleanup)) >= 100
  end

  test "reserves flush time even when the request wait runs to its deadline" do
    deadline = System.monotonic_time(:millisecond) + 1_000
    parent = self()

    stub(IngressDrain, :run, fn request_deadline, _opts ->
      send(parent, {:quiesce, request_deadline})
      Process.sleep(max(request_deadline - System.monotonic_time(:millisecond), 0))
      :timeout
    end)

    assert Shutdown.run(
             deadline_ms: deadline,
             withdrawal_ms: 0,
             request_timeout_ms: 5_000,
             drain_timeout_ms: 1_000
           ) == :timeout

    assert_receive {:quiesce, cutoff}
    assert cutoff <= deadline - 400
    assert_receive {:phase, {:flush, budget}}
    assert Budget.remaining(Budget.deadline(budget.cleanup)) >= 400
  end

  test "a stalled write permits spool sync and receipt cleanup without draining the queues" do
    parent = self()
    receipt = "stalled-write-spool-receipt"
    :ok = SpoolAck.register(receipt, QueueMod, "queue-url")
    SpoolAck.bump(receipt, 1)
    on_exit(fn -> :ets.delete(:spool_ack, receipt) end)

    stub(QueueMod, :nack, fn _url, handle ->
      send(parent, {:nacked, handle})
      :ok
    end)

    stub(ShutdownDrain, :configured_spool_buffer, fn -> :probe_buffer end)

    stub(ShutdownDrain, :sync_spool_buffer, fn limit, :probe_buffer ->
      send(parent, {:sync, Budget.remaining(Budget.deadline(limit))})
      :synced
    end)

    writer =
      Task.async(fn ->
        IngressGate.with_write(fn ->
          send(parent, :writing)

          receive do
            :finish_write -> :ok
          end
        end)
      end)

    try do
      assert_receive :writing

      assert Shutdown.run(
               deadline_ms: System.monotonic_time(:millisecond) + 600,
               withdrawal_ms: 0,
               request_timeout_ms: 0,
               drain_timeout_ms: 500
             ) == :timeout

      assert_received {:sync, remaining}
      assert remaining > 0
      assert_received {:nacked, ^receipt}
      refute_received {:phase, {:flush, _}}
    after
      send(writer.pid, :finish_write)
      Task.await(writer)
    end
  end

  test "waits for a write in progress before flushing after an ingress error" do
    parent = self()
    stub(IngressDrain, :run, fn _deadline, _opts -> :error end)

    writer =
      Task.async(fn ->
        IngressGate.with_write(fn ->
          send(parent, :writing)

          receive do
            :finish_write -> :ok
          end
        end)
      end)

    assert_receive :writing

    drainer =
      Task.async(fn ->
        Shutdown.run(
          deadline_ms: System.monotonic_time(:millisecond) + 4_000,
          withdrawal_ms: 0,
          request_timeout_ms: 0,
          drain_timeout_ms: 500
        )
      end)

    TestUtils.retry_assert(fn ->
      assert IngressGate.with_write(fn -> :ok end) == {:error, :source_unavailable}
    end)

    refute_receive {:phase, {:flush, _}}
    send(writer.pid, :finish_write)
    assert Task.await(writer) == :ok
    assert Task.await(drainer) == :error
    assert_receive {:phase, {:flush, _}}
  end
end
