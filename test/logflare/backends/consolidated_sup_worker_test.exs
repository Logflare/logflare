defmodule Logflare.Backends.ConsolidatedSupWorkerTest do
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Logflare.Backends
  alias Logflare.Backends.ConsolidatedSup
  alias Logflare.Backends.ConsolidatedSupWorker

  describe "ConsolidatedSupWorker" do
    setup do
      insert(:plan, name: "Free")

      {_source, backend} = setup_clickhouse_test()

      worker = start_supervised!({ConsolidatedSupWorker, [interval: 100]})

      on_exit(fn ->
        ConsolidatedSup.stop_pipeline(backend.id)
      end)

      ConsolidatedSup.stop_pipeline(backend)

      [backend: backend, worker: worker]
    end

    test "starts pipeline for consolidated backend on check", %{backend: backend} do
      refute ConsolidatedSup.pipeline_running?(backend)

      TestUtils.retry_assert(fn ->
        assert ConsolidatedSup.pipeline_running?(backend)
      end)
    end

    test "does not start duplicate pipelines", %{backend: backend} do
      case ConsolidatedSup.start_pipeline(backend) do
        {:ok, _pid} -> :ok
        {:error, {:already_started, _}} -> :ok
      end

      assert ConsolidatedSup.pipeline_running?(backend)
      initial_count = ConsolidatedSup.count_pipelines()

      TestUtils.retry_assert(fn ->
        assert ConsolidatedSup.count_pipelines() == initial_count
      end)
    end

    test "does not restart a disabled backend pipeline", %{backend: backend} do
      assert {:ok, disabled} = Logflare.Backends.update_backend(backend, %{enabled: false})
      refute ConsolidatedSup.pipeline_running?(disabled)

      Process.sleep(250)
      refute ConsolidatedSup.pipeline_running?(disabled)
    end

    test "reconciles persisted state instead of a stale backend cache", %{backend: backend} do
      cache_key = {:get_backend, [backend.id]}
      disabled_backend = %{backend | enabled: false}

      Cachex.put!(Logflare.Backends.Cache, cache_key, {:cached, disabled_backend})

      TestUtils.retry_assert(fn ->
        assert ConsolidatedSup.pipeline_running?(backend)
      end)
    end

    test "continues reconciling after one backend fails", %{
      backend: failing_backend,
      worker: worker
    } do
      :sys.suspend(worker)
      {_source, healthy_backend} = setup_clickhouse_test()
      ConsolidatedSup.stop_pipeline(failing_backend)
      ConsolidatedSup.stop_pipeline(healthy_backend)
      test_pid = self()

      stub(Backends, :reconcile_backend_local, fn backend_id ->
        if backend_id == failing_backend.id do
          send(test_pid, {:backend_reconciliation_failed, backend_id})
          raise "persistent pipeline failure"
        end

        send(test_pid, {:backend_reconciled, backend_id})
        :ok
      end)

      log =
        capture_log(fn ->
          :sys.resume(worker)
          assert_receive {:backend_reconciliation_failed, backend_id}, 500
          assert backend_id == failing_backend.id
          assert_receive {:backend_reconciled, backend_id}, 500
          assert backend_id == healthy_backend.id
          Process.sleep(20)
        end)

      assert log =~ "Failed to reconcile consolidated backend"
      assert Process.alive?(worker)
    end
  end

  describe "ConsolidatedSupWorker orphan cleanup" do
    setup do
      insert(:plan, name: "Free")
      user = insert(:user)

      {:ok, backend} =
        Logflare.Backends.create_backend(user, %{
          type: :clickhouse,
          name: "Orphan Test Backend",
          config: %{
            url: "http://localhost",
            port: 8123,
            database: "test_db",
            username: "user",
            password: "pass"
          }
        })

      on_exit(fn ->
        ConsolidatedSup.stop_pipeline(backend.id)
      end)

      [backend: backend, user: user]
    end

    test "stops orphaned pipeline when backend is deleted", %{backend: backend} do
      assert ConsolidatedSup.pipeline_running?(backend)

      assert {:ok, _} = Logflare.Backends.delete_backend(backend)

      refute ConsolidatedSup.pipeline_running?(backend.id)
    end
  end
end
