defmodule Logflare.Backends.ConsolidatedSupWorkerTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.ConsolidatedSup
  alias Logflare.Backends.ConsolidatedSupWorker
  alias Logflare.TestSupport.FakeConsolidatedAdaptor

  describe "ConsolidatedSupWorker" do
    setup do
      insert(:plan, name: "Free")
      user = insert(:user)

      backend =
        insert(:backend,
          type: :webhook,
          user: user,
          config: %{url: "http://example.com"}
        )

      stub(Adaptor, :get_adaptor, fn _backend -> FakeConsolidatedAdaptor end)
      stub(Adaptor, :consolidated_ingest?, fn _backend -> true end)

      ConsolidatedSup.stop_pipeline(backend)

      on_exit(fn ->
        ConsolidatedSup.stop_pipeline(backend.id)
      end)

      [backend: backend]
    end

    test "starts pipeline for consolidated backend on check", %{backend: backend} do
      refute ConsolidatedSup.pipeline_running?(backend)

      send(Process.whereis(ConsolidatedSupWorker), :check)
      Process.sleep(100)

      assert ConsolidatedSup.pipeline_running?(backend)
    end

    test "does not start duplicate pipelines", %{backend: backend} do
      case ConsolidatedSup.start_pipeline(backend) do
        {:ok, _pid} -> :ok
        {:error, {:already_started, _}} -> :ok
      end

      assert ConsolidatedSup.pipeline_running?(backend)
      initial_count = ConsolidatedSup.count_pipelines()

      send(Process.whereis(ConsolidatedSupWorker), :check)
      Process.sleep(100)

      assert ConsolidatedSup.count_pipelines() == initial_count
    end
  end

  describe "ConsolidatedSupWorker orphan cleanup" do
    setup do
      insert(:plan, name: "Free")
      user = insert(:user)

      stub(Adaptor, :get_adaptor, fn _backend -> FakeConsolidatedAdaptor end)
      stub(Adaptor, :consolidated_ingest?, fn _backend -> true end)

      {:ok, backend} =
        Backends.create_backend(%{
          type: :webhook,
          user_id: user.id,
          name: "Orphan Test Backend",
          config: %{url: "http://example.com"}
        })

      on_exit(fn ->
        ConsolidatedSup.stop_pipeline(backend.id)
      end)

      [backend: backend, user: user]
    end

    test "stops orphaned pipeline when backend is deleted", %{backend: backend} do
      assert ConsolidatedSup.pipeline_running?(backend)

      assert {:ok, _} = Backends.delete_backend(backend)

      refute ConsolidatedSup.pipeline_running?(backend.id)
    end
  end
end
