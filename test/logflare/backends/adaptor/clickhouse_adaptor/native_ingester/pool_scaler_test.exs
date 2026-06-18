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

    test "round-robins across active pool indexes", %{backend: backend} do
      {:ok, pid} = start_supervised({PoolScaler, backend})

      :sys.replace_state(pid, fn state ->
        new_indexes = [0, 1, 2]
        :persistent_term.put({:logflare_ch_pool_scaler_indexes, backend.id}, new_indexes)
        %{state | active_indexes: new_indexes}
      end)

      results = Enum.map(1..30, fn _ -> PoolScaler.pick_pool(backend) end)
      assert Enum.all?(results, &(&1 in [0, 1, 2]))
      assert length(Enum.uniq(results)) > 1
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

    test "timeout sample triggers immediate scale evaluation", %{backend: backend} do
      Mimic.stub(PoolSup, :start_pool, fn _backend, _index -> :ok end)

      {:ok, pid} = start_supervised({PoolScaler, backend})
      initial_state = :sys.get_state(pid)
      assert length(initial_state.active_indexes) == 1

      PoolScaler.record_sample(backend, 15_000_000, :timeout)
      :sys.get_state(pid)

      state = :sys.get_state(pid)
      assert length(state.active_indexes) >= 2
    end
  end

  describe "scale up" do
    test "scales up when p95 wait exceeds threshold", %{backend: backend} do
      Mimic.stub(PoolSup, :start_pool, fn _backend, _index -> :ok end)

      {:ok, pid} = start_supervised({PoolScaler, backend})

      for _ <- 1..100 do
        PoolScaler.record_sample(backend, 300_000, :ok)
      end

      send(pid, :tick)
      :sys.get_state(pid)

      state = :sys.get_state(pid)
      assert length(state.active_indexes) >= 2
    end

    test "multi-step scale-up: large overshoot adds multiple pools at once", %{backend: backend} do
      Mimic.stub(PoolSup, :start_pool, fn _backend, _index -> :ok end)

      {:ok, pid} = start_supervised({PoolScaler, backend})

      # Threshold is 250ms, send samples 4x above -> step should be ≥ 4
      for _ <- 1..100 do
        PoolScaler.record_sample(backend, 1_200_000, :ok)
      end

      send(pid, :tick)
      :sys.get_state(pid)

      state = :sys.get_state(pid)
      # max_pool_count defaults to 4, started with 1 — should jump to max in one tick
      assert length(state.active_indexes) == 4
    end

    test "does not scale above max_pool_count", %{backend: backend} do
      Mimic.stub(PoolSup, :start_pool, fn _backend, _index -> :ok end)

      {:ok, pid} = start_supervised({PoolScaler, backend})

      max = Application.fetch_env!(:logflare, :clickhouse_backend_adaptor)[:max_pool_count]

      :sys.replace_state(pid, fn state ->
        indexes = Enum.to_list(0..(max - 1))
        :persistent_term.put({:logflare_ch_pool_scaler_indexes, backend.id}, indexes)
        %{state | active_indexes: indexes}
      end)

      for _ <- 1..100 do
        PoolScaler.record_sample(backend, 1_000_000, :ok)
      end

      send(pid, :tick)
      :sys.get_state(pid)

      state = :sys.get_state(pid)
      assert length(state.active_indexes) == max
    end
  end

  describe "scale down" do
    test "scales down when signal is below threshold after cooldown", %{backend: backend} do
      Mimic.stub(PoolSup, :stop_pool, fn _backend, _index -> :ok end)

      {:ok, pid} = start_supervised({PoolScaler, backend})

      past = System.monotonic_time(:millisecond) - 30_000

      :sys.replace_state(pid, fn state ->
        indexes = [0, 1]
        :persistent_term.put({:logflare_ch_pool_scaler_indexes, backend.id}, indexes)
        %{state | active_indexes: indexes, last_scale_down_at: past}
      end)

      for _ <- 1..100 do
        PoolScaler.record_sample(backend, 5_000, :ok)
      end

      send(pid, :tick)
      :sys.get_state(pid)

      state = :sys.get_state(pid)
      assert state.active_indexes == [0]
    end

    test "respects cooldown and does not scale down twice within window", %{backend: backend} do
      Mimic.stub(PoolSup, :stop_pool, fn _backend, _index -> :ok end)

      {:ok, pid} = start_supervised({PoolScaler, backend})

      recent = System.monotonic_time(:millisecond)

      :sys.replace_state(pid, fn state ->
        indexes = [0, 1]
        :persistent_term.put({:logflare_ch_pool_scaler_indexes, backend.id}, indexes)
        %{state | active_indexes: indexes, last_scale_down_at: recent}
      end)

      for _ <- 1..100 do
        PoolScaler.record_sample(backend, 5_000, :ok)
      end

      send(pid, :tick)
      :sys.get_state(pid)

      state = :sys.get_state(pid)
      assert length(state.active_indexes) == 2
    end

    test "does not scale below min_pool_count of 1", %{backend: backend} do
      Mimic.stub(PoolSup, :stop_pool, fn _backend, _index -> :ok end)

      {:ok, pid} = start_supervised({PoolScaler, backend})

      past = System.monotonic_time(:millisecond) - 30_000
      :sys.replace_state(pid, fn state -> %{state | last_scale_down_at: past} end)

      for _ <- 1..100 do
        PoolScaler.record_sample(backend, 5_000, :ok)
      end

      send(pid, :tick)
      :sys.get_state(pid)

      state = :sys.get_state(pid)
      assert state.active_indexes == [0]
    end

    test "aggressively sheds multiple pools at once when very idle", %{backend: backend} do
      Mimic.stub(PoolSup, :stop_pool, fn _backend, _index -> :ok end)

      {:ok, pid} = start_supervised({PoolScaler, backend})

      past = System.monotonic_time(:millisecond) - 30_000

      :sys.replace_state(pid, fn state ->
        indexes = [0, 1, 2, 3]
        :persistent_term.put({:logflare_ch_pool_scaler_indexes, backend.id}, indexes)
        %{state | active_indexes: indexes, last_scale_down_at: past}
      end)

      # Threshold is 25ms; samples well below 25%/4 ~= 6ms trigger half-shed.
      for _ <- 1..100 do
        PoolScaler.record_sample(backend, 1_000, :ok)
      end

      send(pid, :tick)
      :sys.get_state(pid)

      state = :sys.get_state(pid)
      assert length(state.active_indexes) <= 2
    end
  end

  describe "telemetry" do
    test "emits scale telemetry on scale up with direction and step", %{backend: backend} do
      Mimic.stub(PoolSup, :start_pool, fn _backend, _index -> :ok end)

      {:ok, pid} = start_supervised({PoolScaler, backend})

      test_pid = self()

      :telemetry.attach(
        "test_scale_up_#{backend.id}",
        [:logflare, :backends, :clickhouse, :pool, :scale],
        fn _event, measurements, meta, _config ->
          send(test_pid, {:scale_event, meta[:direction], measurements[:step]})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach("test_scale_up_#{backend.id}") end)

      for _ <- 1..100 do
        PoolScaler.record_sample(backend, 300_000, :ok)
      end

      send(pid, :tick)
      :sys.get_state(pid)

      assert_received {:scale_event, :up, step} when step >= 1
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
