defmodule Logflare.Backends.Spool.SpoolAckTest do
  use ExUnit.Case, async: false

  import Mimic

  alias Logflare.Backends.Spool.Queue.PubSub, as: QueueMod
  alias Logflare.Backends.Spool.SpoolAck
  alias Logflare.Backends.Spool.Storage.GCS, as: StorageMod
  alias Logflare.TestUtils

  setup :set_mimic_global

  setup do
    :ets.delete_all_objects(:spool_ack)
    on_exit(fn -> :ets.delete_all_objects(:spool_ack) end)
    :ok
  end

  defp unique_handle, do: "handle-#{System.unique_integer([:positive])}"

  describe "deleting the spool object" do
    setup do
      original = Application.get_env(:logflare, :spool, [])
      on_exit(fn -> Application.put_env(:logflare, :spool, original) end)
      :ets.delete_all_objects(:spool_object_deleter)
      handle = unique_handle()
      test_pid = self()

      stub(StorageMod, :delete, fn bucket, key ->
        send(test_pid, {:deleted, bucket, key}) && :ok
      end)

      %{handle: handle, test_pid: test_pid}
    end

    test "deletes the object after the final ack succeeds", %{handle: handle} do
      stub(QueueMod, :ack, fn _url, _h -> :ok end)

      SpoolAck.register(handle, QueueMod, "queue-url", {StorageMod, "bucket", "0/a.etf"})
      SpoolAck.bump(handle, 2)
      SpoolAck.ack(handle, 1)
      refute_receive {:deleted, _, _}, 100

      SpoolAck.ack(handle, 1)
      assert_receive {:deleted, "bucket", "0/a.etf"}
    end

    test "does not delete when the queue ack fails", %{handle: handle} do
      stub(QueueMod, :ack, fn _url, _h -> {:error, :boom} end)

      SpoolAck.register(handle, QueueMod, "queue-url", {StorageMod, "bucket", "0/a.etf"})
      SpoolAck.bump(handle, 1)
      SpoolAck.ack(handle, 1)

      refute_receive {:deleted, _, _}, 200
    end

    test "does not delete when delete_on_ack is disabled", %{handle: handle} do
      Application.put_env(:logflare, :spool, delete_on_ack: false)
      stub(QueueMod, :ack, fn _url, _h -> :ok end)

      SpoolAck.register(handle, QueueMod, "queue-url", {StorageMod, "bucket", "0/a.etf"})
      SpoolAck.bump(handle, 1)
      SpoolAck.ack(handle, 1)

      refute_receive {:deleted, _, _}, 200
    end

    test "does not delete when nacking pending handles", %{handle: handle} do
      stub(QueueMod, :nack, fn _url, _h -> :ok end)

      SpoolAck.register(handle, QueueMod, "queue-url", {StorageMod, "bucket", "0/a.etf"})
      SpoolAck.bump(handle, 1)

      assert SpoolAck.nack_pending(1_000) == :ok
      refute_receive {:deleted, _, _}, 200
    end

    test "does not delete swept stale rows even with ack_stale", %{handle: handle} do
      Application.put_env(:logflare, SpoolAck, ack_stale: true)
      on_exit(fn -> Application.delete_env(:logflare, SpoolAck) end)
      stub(QueueMod, :ack, fn _url, _h -> :ok end)

      SpoolAck.register(handle, QueueMod, "queue-url", {StorageMod, "bucket", "0/a.etf"})
      Process.sleep(5)
      SpoolAck.sweep_stale(0)
      SpoolAck.flush(1_000)

      refute_receive {:deleted, _, _}, 200
    end

    test "a failed delete is reported and does not crash SpoolAck", %{handle: handle} do
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :storage, :delete])
      stub(QueueMod, :ack, fn _url, _h -> :ok end)
      stub(StorageMod, :delete, fn _b, _k -> {:error, :boom} end)
      spool_ack = Process.whereis(SpoolAck)

      SpoolAck.register(handle, QueueMod, "queue-url", {StorageMod, "bucket", "0/a.etf"})
      SpoolAck.bump(handle, 1)
      SpoolAck.ack(handle, 1)

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :storage, :delete], _,
                      %{result: :error}}

      assert Process.whereis(SpoolAck) == spool_ack
    end

    test "a hanging delete does not block later acks", %{handle: handle} do
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :storage, :delete])
      Application.put_env(:logflare, :spool, delete_timeout_ms: 100)
      other = unique_handle()
      test_pid = self()
      stub(StorageMod, :delete, fn _b, _k -> Process.sleep(:infinity) end)
      stub(QueueMod, :ack, fn _url, h -> send(test_pid, {:acked, h}) && :ok end)

      for h <- [handle, other] do
        SpoolAck.register(h, QueueMod, "queue-url", {StorageMod, "bucket", "key-#{h}"})
        SpoolAck.bump(h, 1)
        SpoolAck.ack(h, 1)
      end

      assert_receive {:acked, ^handle}
      assert_receive {:acked, ^other}
      assert SpoolAck.flush(1_000) == :ok

      for _ <- 1..2 do
        assert_receive {:telemetry_event, [:logflare, :backends, :spool, :storage, :delete], _,
                        %{result: :timeout}},
                       1_000
      end
    end
  end

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
      assert SpoolAck.lookup(handle) == :error
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

  describe "sweep_stale/1 with ack_stale: false" do
    setup do
      Application.put_env(:logflare, SpoolAck, ack_stale: false)
      on_exit(fn -> Application.put_env(:logflare, SpoolAck, ack_stale: true) end)
    end

    test "deletes a row older than the given threshold without performing a queue ack" do
      handle = unique_handle()
      test_pid = self()
      stub(QueueMod, :ack, fn _url, h -> send(test_pid, {:acked, h}) end)

      SpoolAck.register(handle, QueueMod, "queue-url")
      SpoolAck.bump(handle, 3)
      Process.sleep(5)

      assert :ok = SpoolAck.sweep_stale(0)

      assert SpoolAck.lookup(handle) == :error
      refute_receive {:acked, ^handle}, 200
    end

    test "leaves a row younger than the given threshold alone" do
      handle = unique_handle()
      SpoolAck.register(handle, QueueMod, "queue-url")
      SpoolAck.bump(handle, 1)

      assert :ok = SpoolAck.sweep_stale(:timer.minutes(10))

      assert {:ok, %{count: 1, queue_mod: QueueMod, queue_url: "queue-url"}} =
               SpoolAck.lookup(handle)
    end
  end

  describe "sweep_stale/1 with ack_stale: true" do
    setup do
      Application.put_env(:logflare, SpoolAck, ack_stale: true)
      on_exit(fn -> Application.put_env(:logflare, SpoolAck, ack_stale: true) end)
    end

    test "performs the real queue ack for a stale row before deleting it" do
      handle = unique_handle()
      test_pid = self()

      stub(QueueMod, :ack, fn url, h ->
        send(test_pid, {:acked, url, h})
        :ok
      end)

      SpoolAck.register(handle, QueueMod, "queue-url")
      SpoolAck.bump(handle, 3)
      Process.sleep(5)

      assert :ok = SpoolAck.sweep_stale(0)

      assert_receive {:acked, "queue-url", ^handle}
      assert SpoolAck.lookup(handle) == :error
    end

    test "leaves a row younger than the given threshold alone" do
      handle = unique_handle()
      test_pid = self()
      stub(QueueMod, :ack, fn _url, h -> send(test_pid, {:acked, h}) end)

      SpoolAck.register(handle, QueueMod, "queue-url")
      SpoolAck.bump(handle, 1)

      assert :ok = SpoolAck.sweep_stale(:timer.minutes(10))

      refute_receive {:acked, ^handle}, 200

      assert {:ok, %{count: 1, queue_mod: QueueMod, queue_url: "queue-url"}} =
               SpoolAck.lookup(handle)
    end
  end

  describe "pending_count/0" do
    test "counts handles still waiting on outstanding work" do
      assert SpoolAck.pending_count() == 0

      SpoolAck.register(unique_handle(), QueueMod, "queue-url")
      SpoolAck.register(unique_handle(), QueueMod, "queue-url")

      assert SpoolAck.pending_count() == 2
    end
  end

  describe "nack_pending/1" do
    test "nacks every pending handle through its own queue and forgets it" do
      test_pid = self()
      [h1, h2] = [unique_handle(), unique_handle()]

      stub(QueueMod, :nack, fn url, h ->
        send(test_pid, {:nacked, url, h})
        :ok
      end)

      stub(QueueMod, :ack, fn _url, h -> send(test_pid, {:acked, h}) end)

      SpoolAck.register(h1, QueueMod, "queue-1")
      SpoolAck.bump(h1, 2)
      SpoolAck.register(h2, QueueMod, "queue-2")

      assert SpoolAck.nack_pending(1_000) == :ok

      assert_received {:nacked, "queue-1", ^h1}
      assert_received {:nacked, "queue-2", ^h2}

      assert SpoolAck.pending_count() == 0

      SpoolAck.ack(h1, 2)
      refute_receive {:acked, ^h1}, 200
    end

    test "nacks pending handles concurrently rather than one request at a time" do
      stub(QueueMod, :nack, fn _url, _h ->
        Process.sleep(200)
        :ok
      end)

      for _ <- 1..5, do: SpoolAck.register(unique_handle(), QueueMod, "queue-url")

      {elapsed_us, :ok} = :timer.tc(fn -> SpoolAck.nack_pending(5_000) end)

      assert elapsed_us < 600_000
    end

    test "a nack that hangs is killed after its own timeout, freeing its slot for the rest" do
      test_pid = self()
      blocked = for _ <- 1..16, do: unique_handle()
      other = unique_handle()

      stub(QueueMod, :nack, fn _url, h ->
        if h in blocked, do: Process.sleep(:infinity)
        send(test_pid, {:nacked, h})
        :ok
      end)

      for h <- blocked, do: SpoolAck.register(h, QueueMod, "queue-url")
      SpoolAck.register(other, QueueMod, "queue-url")

      {elapsed_us, result} =
        :timer.tc(fn -> SpoolAck.nack_pending(5_000, nack_timeout_ms: 100) end)

      assert result == :ok
      assert elapsed_us < 1_000_000
      assert_received {:nacked, ^other}
    end

    test "a blocked nack cannot hold shutdown past the deadline" do
      test_pid = self()
      [blocked, other] = [unique_handle(), unique_handle()]

      stub(QueueMod, :nack, fn
        _url, ^blocked ->
          Process.sleep(:infinity)

        _url, h ->
          send(test_pid, {:nacked, h})
          :ok
      end)

      SpoolAck.register(blocked, QueueMod, "queue-url")
      SpoolAck.register(other, QueueMod, "queue-url")

      {elapsed_us, result} = :timer.tc(fn -> SpoolAck.nack_pending(200) end)

      assert result == :timeout
      assert elapsed_us < 500_000
      assert_received {:nacked, ^other}
    end
  end

  describe "flush/1" do
    test "returns only after queue acks already handed to SpoolAck have been sent" do
      handle = unique_handle()
      test_pid = self()

      stub(QueueMod, :ack, fn _url, h ->
        Process.sleep(200)
        send(test_pid, {:acked, h})
        :ok
      end)

      SpoolAck.register(handle, QueueMod, "queue-url")
      SpoolAck.bump(handle, 1)
      SpoolAck.ack(handle, 1)

      assert SpoolAck.flush(1_000) == :ok
      assert_received {:acked, ^handle}
    end

    test "gives up at the deadline instead of exiting the caller" do
      handle = unique_handle()

      stub(QueueMod, :ack, fn _url, _h ->
        Process.sleep(500)
        :ok
      end)

      SpoolAck.register(handle, QueueMod, "queue-url")
      SpoolAck.bump(handle, 1)
      SpoolAck.ack(handle, 1)

      assert SpoolAck.flush(100) == :timeout

      SpoolAck.flush(1_000)
    end
  end
end
