defmodule Logflare.Backends.ConsolidatedSupWorkerTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.ConsolidatedSup
  alias Logflare.Backends.ConsolidatedSupWorker

  describe "ConsolidatedSupWorker" do
    setup do
      insert(:plan, name: "Free")

      {_source, backend, cleanup_fn} = setup_clickhouse_test()

      on_exit(fn ->
        ConsolidatedSup.stop_pipeline(backend.id)
        cleanup_fn.()
      end)

      ConsolidatedSup.stop_pipeline(backend)

      [backend: backend]
    end

    test "starts pipeline for consolidated backend on check", %{backend: backend} do
      refute ConsolidatedSup.pipeline_running?(backend)

      send(Process.whereis(ConsolidatedSupWorker), :check)

      TestUtils.retry_assert([sleep: 100], fn ->
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

      send(Process.whereis(ConsolidatedSupWorker), :check)

      TestUtils.retry_assert([sleep: 100], fn ->
        assert ConsolidatedSup.count_pipelines() == initial_count
      end)
    end
  end

  describe "ConsolidatedSupWorker orphan cleanup" do
    setup do
      insert(:plan, name: "Free")
      user = insert(:user)

      {:ok, backend} =
        Logflare.Backends.create_backend(%{
          type: :clickhouse,
          user_id: user.id,
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
