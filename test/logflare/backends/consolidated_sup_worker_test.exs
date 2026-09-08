defmodule Logflare.Backends.ConsolidatedSupWorkerTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.ConsolidatedSup
  alias Logflare.Backends.ConsolidatedSupWorker

  describe "ConsolidatedSupWorker" do
    setup do
      insert(:plan, name: "Free")

      {_source, backend} = setup_clickhouse_test()

      start_supervised!({ConsolidatedSupWorker, [interval: 100]})

      on_exit(fn ->
        ConsolidatedSup.stop_pipeline(backend.id)
      end)

      ConsolidatedSup.stop_pipeline(backend)

      [backend: backend]
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

  describe "ConsolidatedSupWorker single-tenant default" do
    TestUtils.setup_single_tenant(seed_user: true, backend_type: :clickhouse)

    test "starts the synthetic consolidated backend without sources or rules" do
      backend = Backends.get_single_tenant_default_backend()
      assert backend.id == 0

      on_exit(fn -> ConsolidatedSup.stop_pipeline(backend.id) end)

      refute ConsolidatedSup.pipeline_running?(backend.id)
      worker = start_supervised!({ConsolidatedSupWorker, [interval: 10_000]})

      send(worker, :check)

      TestUtils.retry_assert(fn ->
        assert ConsolidatedSup.pipeline_running?(backend.id)
      end)

      assert Backends.list_backends(user_id: backend.user_id) == []
    end
  end
end
