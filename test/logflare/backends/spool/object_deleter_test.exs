defmodule Logflare.Backends.Spool.ObjectDeleterTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import Mimic

  alias Logflare.Backends.Spool.ObjectDeleter
  alias Logflare.Backends.Spool.Storage.GCS, as: StorageMod
  alias Logflare.TestUtils

  setup :set_mimic_global

  setup do
    original = Application.get_env(:logflare, :spool, [])
    on_exit(fn -> Application.put_env(:logflare, :spool, original) end)
    :ets.delete_all_objects(:spool_object_deleter)
    :ok
  end

  defp put_spool_config(opts) do
    Application.put_env(
      :logflare,
      :spool,
      Keyword.merge(Application.get_env(:logflare, :spool, []), opts)
    )
  end

  describe "flush/1" do
    test "returns once every buffered delete has been attempted" do
      test_pid = self()

      stub(StorageMod, :delete, fn _bucket, key ->
        Process.sleep(50)
        send(test_pid, {:deleted, key})
        :ok
      end)

      for i <- 1..3, do: ObjectDeleter.enqueue({StorageMod, "bucket", "key-#{i}"})

      assert ObjectDeleter.flush(2_000) == :ok

      for i <- 1..3 do
        key = "key-#{i}"
        assert_received {:deleted, ^key}
      end

      assert ObjectDeleter.pending_count() == 0
    end

    test "returns :timeout when the deletes take longer than asked" do
      stub(StorageMod, :delete, fn _bucket, _key ->
        Process.sleep(300)
        :ok
      end)

      ObjectDeleter.enqueue({StorageMod, "bucket", "slow"})

      assert ObjectDeleter.flush(20) == :timeout
      assert ObjectDeleter.flush(2_000) == :ok
    end
  end

  describe "failure logging" do
    test "logs failures at most once per interval, however many deletes fail" do
      stub(StorageMod, :delete, fn _bucket, _key -> {:error, :forbidden} end)
      :sys.replace_state(ObjectDeleter, fn state -> %{state | last_log_at: nil, failed: %{}} end)

      log =
        capture_log([level: :warning], fn ->
          for i <- 1..5 do
            ObjectDeleter.enqueue({StorageMod, "bucket", "key-#{i}"})
            assert ObjectDeleter.flush(2_000) == :ok
          end
        end)

      assert length(String.split(log, "some spool object deletes failed")) == 2
    end

    test "logs the failures accumulated while the log was suppressed once the interval passes" do
      stub(StorageMod, :delete, fn _bucket, _key -> {:error, :forbidden} end)

      :sys.replace_state(ObjectDeleter, fn state ->
        %{state | last_log_at: System.monotonic_time(:millisecond), failed: %{error: 4}}
      end)

      log =
        capture_log([level: :warning, metadata: [:error_string]], fn ->
          :sys.replace_state(ObjectDeleter, fn state ->
            %{state | last_log_at: System.monotonic_time(:millisecond) - 60_000}
          end)

          ObjectDeleter.enqueue({StorageMod, "bucket", "key"})
          assert ObjectDeleter.flush(2_000) == :ok
        end)

      assert log =~ "error: 5"
    end
  end

  describe "enqueue/1" do
    test "deletes the object in the background" do
      test_pid = self()

      stub(StorageMod, :delete, fn bucket, key ->
        send(test_pid, {:deleted, bucket, key}) && :ok
      end)

      assert ObjectDeleter.enqueue({StorageMod, "bucket", "0/a.etf"}) == :ok

      assert_receive {:deleted, "bucket", "0/a.etf"}
    end

    test "never runs more deletes at once than delete_concurrency" do
      put_spool_config(delete_concurrency: 2)
      test_pid = self()
      counter = :counters.new(2, [])

      stub(StorageMod, :delete, fn _bucket, key ->
        :counters.add(counter, 1, 1)
        :counters.put(counter, 2, max(:counters.get(counter, 2), :counters.get(counter, 1)))
        Process.sleep(50)
        :counters.sub(counter, 1, 1)
        send(test_pid, {:deleted, key})
        :ok
      end)

      for i <- 1..6, do: ObjectDeleter.enqueue({StorageMod, "bucket", "key-#{i}"})

      for i <- 1..6 do
        key = "key-#{i}"
        assert_receive {:deleted, ^key}, 1_000
      end

      assert :counters.get(counter, 2) <= 2
    end

    test "drops instead of raising when the buffer table is gone" do
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :delete_buffer, :dropped])
      :ets.rename(:spool_object_deleter, :spool_object_deleter_moved)
      on_exit(fn -> :ets.rename(:spool_object_deleter_moved, :spool_object_deleter) end)

      assert ObjectDeleter.enqueue({StorageMod, "bucket", "0/a.etf"}) == :dropped

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :delete_buffer, :dropped],
                      %{count: 1}, %{reason: :not_running}}
    end

    test "drops new objects once delete_buffer_max is reached" do
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :delete_buffer, :dropped])
      put_spool_config(delete_buffer_max: 0)
      test_pid = self()
      stub(StorageMod, :delete, fn _b, key -> send(test_pid, {:deleted, key}) && :ok end)

      assert ObjectDeleter.enqueue({StorageMod, "bucket", "0/a.etf"}) == :dropped

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :delete_buffer, :dropped],
                      %{count: 1}, %{reason: :buffer_full}}

      refute_receive {:deleted, _}, 200
    end

    test "kills a hanging delete at delete_timeout_ms and keeps draining" do
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :storage, :delete])
      put_spool_config(delete_timeout_ms: 50, delete_concurrency: 1)
      test_pid = self()

      stub(StorageMod, :delete, fn
        _bucket, "hang" -> Process.sleep(:infinity)
        _bucket, key -> send(test_pid, {:deleted, key}) && :ok
      end)

      ObjectDeleter.enqueue({StorageMod, "bucket", "hang"})
      ObjectDeleter.enqueue({StorageMod, "bucket", "fine"})

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :storage, :delete], _,
                      %{result: :timeout}}

      assert_receive {:deleted, "fine"}
    end

    test "reports failed and crashing deletes as errors without dying" do
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :storage, :delete])
      worker = Process.whereis(ObjectDeleter)

      stub(StorageMod, :delete, fn
        _bucket, "crash" -> raise "boom"
        _bucket, _key -> {:error, :forbidden}
      end)

      ObjectDeleter.enqueue({StorageMod, "bucket", "crash"})
      ObjectDeleter.enqueue({StorageMod, "bucket", "denied"})

      for _ <- 1..2 do
        assert_receive {:telemetry_event, [:logflare, :backends, :spool, :storage, :delete], _,
                        %{result: :error}}
      end

      assert Process.whereis(ObjectDeleter) == worker
    end
  end
end
