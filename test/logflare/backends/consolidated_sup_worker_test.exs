defmodule Logflare.Backends.ConsolidatedSupWorkerTest do
  use Logflare.DataCase, async: false

  import Ecto.Query

  alias Logflare.Backends
  alias Logflare.Backends.ConsolidatedSup
  alias Logflare.Backends.ConsolidatedSupWorker
  alias Logflare.Repo

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

    test "starts a pipeline for a default ingest backend with no linked sources", %{
      backend: backend
    } do
      {:ok, _} = Backends.update_backend(backend, %{default_ingest?: true})

      Repo.delete_all(from(sb in "sources_backends", where: sb.backend_id == ^backend.id))

      ConsolidatedSup.stop_pipeline(backend)

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
end
