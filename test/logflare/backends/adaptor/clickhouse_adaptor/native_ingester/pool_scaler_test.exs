defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.PoolScalerTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.PoolScaler
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.PoolSup

  setup :set_mimic_global

  setup do
    insert(:plan, name: "Free")
    _user = insert(:user)
    backend = insert(:backend, type: :clickhouse, config: clickhouse_config())
    [backend: backend]
  end

  describe "pick_pool/1" do
    test "returns 0 when scaler has not started", %{backend: backend} do
      assert PoolScaler.pick_pool(backend) == 0
    end

    test "returns 0 when only one pool is active", %{backend: backend} do
      {:ok, _} = start_supervised({PoolScaler, backend})
      assert PoolScaler.pick_pool(backend) == 0
    end

    test "returns index within active range when multiple pools active", %{backend: backend} do
      {:ok, pid} = start_supervised({PoolScaler, backend})

      # Manually push state with 3 active pools to test distribution
      :sys.replace_state(pid, fn state ->
        new_indexes = [0, 1, 2]
        :persistent_term.put({:logflare_ch_pool_scaler_indexes, backend.id}, new_indexes)
        %{state | active_indexes: new_indexes}
      end)

      results = Enum.map(1..30, fn _ -> PoolScaler.pick_pool(backend) end)
      assert Enum.all?(results, &(&1 in [0, 1, 2]))
      # With 30 calls and 3 options, we expect all 3 to appear (probabilistic)
      assert Enum.uniq(results) |> length() > 1
    end
  end

  describe "record_sample/3" do
    test "returns :ok silently when scaler is not running", %{backend: backend} do
      assert :ok = PoolScaler.record_sample(backend, 1_000, :ok)
    end

    test "records samples without crashing", %{backend: backend} do
      {:ok, _} = start_supervised({PoolScaler, backend})
      assert :ok = PoolScaler.record_sample(backend, 5_000, :ok)
      assert :ok = PoolScaler.record_sample(backend, 0, :timeout)
    end
  end

  describe "scale up" do
    test "scales up when p95 wait exceeds threshold", %{backend: backend} do
      Mimic.stub(PoolSup, :start_pool, fn _backend, _index -> :ok end)

      {:ok, pid} = start_supervised({PoolScaler, backend})

      # Inject high-latency samples (600ms = 600_000us, threshold is 500ms)
      for _ <- 1..100 do
        PoolScaler.record_sample(backend, 600_000, :ok)
      end

      # Trigger scale tick
      send(pid, :scale)
      # Wait for the cast to be processed
      :sys.get_state(pid)

      state = :sys.get_state(pid)
      assert length(state.active_indexes) == 2
    end

    test "scales up immediately on checkout timeout", %{backend: backend} do
      Mimic.stub(PoolSup, :start_pool, fn _backend, _index -> :ok end)

      {:ok, pid} = start_supervised({PoolScaler, backend})

      # A single timeout sample is enough to trigger scale-up
      PoolScaler.record_sample(backend, 15_000_000, :timeout)

      send(pid, :scale)
      :sys.get_state(pid)

      state = :sys.get_state(pid)
      assert length(state.active_indexes) == 2
    end

    test "does not scale above max_pool_count", %{backend: backend} do
      Mimic.stub(PoolSup, :start_pool, fn _backend, _index -> :ok end)

      {:ok, pid} = start_supervised({PoolScaler, backend})

      # Push state with max-1 active pools, high latency
      max = Application.fetch_env!(:logflare, :clickhouse_backend_adaptor)[:max_pool_count]

      :sys.replace_state(pid, fn state ->
        indexes = Enum.to_list(0..(max - 1))
        :persistent_term.put({:logflare_ch_pool_scaler_indexes, backend.id}, indexes)
        %{state | active_indexes: indexes}
      end)

      for _ <- 1..100 do
        PoolScaler.record_sample(backend, 600_000, :ok)
      end

      send(pid, :scale)
      :sys.get_state(pid)

      state = :sys.get_state(pid)
      assert length(state.active_indexes) == max
    end
  end

  describe "scale down" do
    test "scales down when p95 wait is below threshold after cooldown", %{backend: backend} do
      Mimic.stub(PoolSup, :stop_pool, fn _backend, _index -> :ok end)

      {:ok, pid} = start_supervised({PoolScaler, backend})

      # Seed state with 2 pools and a past scale-down timestamp (beyond cooldown)
      past = System.monotonic_time(:millisecond) - 60_000

      :sys.replace_state(pid, fn state ->
        indexes = [0, 1]
        :persistent_term.put({:logflare_ch_pool_scaler_indexes, backend.id}, indexes)
        %{state | active_indexes: indexes, last_scale_down_at: past}
      end)

      # Inject low-latency samples (10ms = 10_000us, threshold is 50ms)
      for _ <- 1..100 do
        PoolScaler.record_sample(backend, 10_000, :ok)
      end

      send(pid, :scale)
      :sys.get_state(pid)

      state = :sys.get_state(pid)
      assert length(state.active_indexes) == 1
      assert state.active_indexes == [0]
    end

    test "respects cooldown and does not scale down twice within window", %{backend: backend} do
      Mimic.stub(PoolSup, :stop_pool, fn _backend, _index -> :ok end)

      {:ok, pid} = start_supervised({PoolScaler, backend})

      # Seed state: 2 pools, recent scale-down (within cooldown window)
      recent = System.monotonic_time(:millisecond)

      :sys.replace_state(pid, fn state ->
        indexes = [0, 1]
        :persistent_term.put({:logflare_ch_pool_scaler_indexes, backend.id}, indexes)
        %{state | active_indexes: indexes, last_scale_down_at: recent}
      end)

      for _ <- 1..100 do
        PoolScaler.record_sample(backend, 10_000, :ok)
      end

      send(pid, :scale)
      :sys.get_state(pid)

      # Should still have 2 pools due to cooldown
      state = :sys.get_state(pid)
      assert length(state.active_indexes) == 2
    end

    test "does not scale below min_pool_count of 1", %{backend: backend} do
      Mimic.stub(PoolSup, :stop_pool, fn _backend, _index -> :ok end)

      {:ok, pid} = start_supervised({PoolScaler, backend})

      past = System.monotonic_time(:millisecond) - 60_000

      :sys.replace_state(pid, fn state ->
        %{state | last_scale_down_at: past}
      end)

      # Only pool 0 active — should not scale further down
      for _ <- 1..100 do
        PoolScaler.record_sample(backend, 10_000, :ok)
      end

      send(pid, :scale)
      :sys.get_state(pid)

      state = :sys.get_state(pid)
      assert state.active_indexes == [0]
    end
  end

  describe "telemetry" do
    test "emits scale telemetry on scale up", %{backend: backend} do
      Mimic.stub(PoolSup, :start_pool, fn _backend, _index -> :ok end)

      {:ok, pid} = start_supervised({PoolScaler, backend})

      test_pid = self()

      :telemetry.attach(
        "test_scale_up_#{backend.id}",
        [:logflare, :backends, :clickhouse, :pool, :scale],
        fn _event, measurements, _meta, _config ->
          send(test_pid, {:scale_event, measurements[:active_pools]})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach("test_scale_up_#{backend.id}") end)

      for _ <- 1..100 do
        PoolScaler.record_sample(backend, 600_000, :ok)
      end

      send(pid, :scale)
      :sys.get_state(pid)

      assert_received {:scale_event, 2}
    end
  end

  describe "persistent_term lifecycle" do
    test "clears persistent_term entry on terminate", %{backend: backend} do
      {:ok, pid} = start_supervised({PoolScaler, backend})
      key = {:logflare_ch_pool_scaler_indexes, backend.id}

      assert :persistent_term.get(key) == [0]

      GenServer.stop(pid)
      Process.sleep(50)

      assert_raise ArgumentError, fn -> :persistent_term.get(key) end
    end
  end

  defp clickhouse_config do
    %{
      url: "http://localhost:8123",
      database: "logflare_test",
      port: 8123,
      username: "default",
      password: ""
    }
  end
end
