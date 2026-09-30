defmodule Logflare.Backends.Spool.SpoolAckTest do
  use ExUnit.Case, async: false

  import Mimic

  alias Logflare.Backends.Spool.Queue.PubSub, as: QueueMod
  alias Logflare.Backends.Spool.SpoolAck

  setup :set_mimic_global

  defp unique_handle, do: "handle-#{System.unique_integer([:positive])}"

  describe "nil handle" do
    test "bump/2 is a no-op" do
      assert SpoolAck.bump(nil, 1) == :ok
      assert SpoolAck.bump(nil, 5) == :ok
    end

    test "ack/2 is a no-op, never performs a queue ack" do
      test_pid = self()
      stub(QueueMod, :ack, fn _url, _handle -> send(test_pid, :ack_called) end)

      assert SpoolAck.ack(nil, 1) == :ok

      refute_receive :ack_called, 200
    end
  end

  describe "register/3, bump/2, ack/2" do
    test "a handle bumped once and acked once performs the real queue ack" do
      handle = unique_handle()
      test_pid = self()

      stub(QueueMod, :ack, fn url, ^handle ->
        send(test_pid, {:acked, url, handle})
        :ok
      end)

      SpoolAck.register(handle, QueueMod, "queue-url")
      SpoolAck.bump(handle, 1)
      SpoolAck.ack(handle, 1)

      assert_receive {:acked, "queue-url", ^handle}
      assert :ets.lookup(:spool_ack, handle) == []
    end

    test "does not ack until every bump has a matching ack" do
      handle = unique_handle()
      test_pid = self()
      stub(QueueMod, :ack, fn _url, h -> send(test_pid, {:acked, h}) end)

      SpoolAck.register(handle, QueueMod, "queue-url")
      SpoolAck.bump(handle, 3)
      SpoolAck.ack(handle, 1)
      SpoolAck.ack(handle, 1)

      refute_receive {:acked, ^handle}, 200

      SpoolAck.ack(handle, 1)

      assert_receive {:acked, ^handle}
    end

    test "multiple bumps require an equal number of acks before the count reaches zero" do
      handle = unique_handle()
      test_pid = self()
      stub(QueueMod, :ack, fn _url, h -> send(test_pid, {:acked, h}) end)

      SpoolAck.register(handle, QueueMod, "queue-url")
      SpoolAck.bump(handle, 1)
      SpoolAck.bump(handle, 1)
      SpoolAck.bump(handle, 1)
      SpoolAck.ack(handle, 1)

      refute_receive {:acked, ^handle}, 200

      SpoolAck.ack(handle, 1)
      refute_receive {:acked, ^handle}, 200

      SpoolAck.ack(handle, 1)
      assert_receive {:acked, ^handle}
    end

    test "ack/2 tolerates the count going negative without acking twice" do
      handle = unique_handle()
      test_pid = self()
      stub(QueueMod, :ack, fn _url, h -> send(test_pid, {:acked, h}) end)

      SpoolAck.register(handle, QueueMod, "queue-url")
      SpoolAck.bump(handle, 1)
      SpoolAck.ack(handle, 1)
      assert_receive {:acked, ^handle}

      # A stray extra ack for an already-resolved handle must not raise or
      # perform a second queue ack -- the row is already gone.
      assert SpoolAck.ack(handle, 1) == :ok
      refute_receive {:acked, ^handle}, 200
    end

    test "bump/2 against a never-registered handle does not raise" do
      assert SpoolAck.bump(unique_handle(), 1) == :ok
    end

    test "ack/2 against a never-registered handle does not raise" do
      assert SpoolAck.ack(unique_handle(), 1) == :ok
    end
  end

  describe "sweep_stale/1" do
    test "deletes a row older than the given threshold without performing a queue ack" do
      handle = unique_handle()
      test_pid = self()
      stub(QueueMod, :ack, fn _url, h -> send(test_pid, {:acked, h}) end)

      SpoolAck.register(handle, QueueMod, "queue-url")
      SpoolAck.bump(handle, 3)
      Process.sleep(5)

      assert :ok = SpoolAck.sweep_stale(0)

      assert :ets.lookup(:spool_ack, handle) == []
      refute_receive {:acked, ^handle}, 200
    end

    test "leaves a row younger than the given threshold alone" do
      handle = unique_handle()
      SpoolAck.register(handle, QueueMod, "queue-url")
      SpoolAck.bump(handle, 1)

      assert :ok = SpoolAck.sweep_stale(:timer.minutes(10))

      assert [{^handle, 1, QueueMod, "queue-url", _registered_at}] =
               :ets.lookup(:spool_ack, handle)
    end
  end
end
