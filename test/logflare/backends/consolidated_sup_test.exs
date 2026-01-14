defmodule Logflare.Backends.ConsolidatedSupTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.ConsolidatedSup
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.TestSupport.FakeConsolidatedAdaptor

  describe "ConsolidatedSup" do
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

      on_exit(fn ->
        ConsolidatedSup.stop_pipeline(backend.id)
      end)

      [backend: backend, user: user]
    end

    test "start_pipeline/1 starts a consolidated pipeline for a backend", %{backend: backend} do
      assert {:ok, _pid} = ConsolidatedSup.start_pipeline(backend)
      assert ConsolidatedSup.pipeline_running?(backend)
    end

    test "start_pipeline/1 returns error if pipeline already started", %{backend: backend} do
      assert {:ok, _pid} = ConsolidatedSup.start_pipeline(backend)
      assert {:error, {:already_started, _}} = ConsolidatedSup.start_pipeline(backend)
    end

    test "stop_pipeline/1 stops a running pipeline", %{backend: backend} do
      assert {:ok, _pid} = ConsolidatedSup.start_pipeline(backend)
      assert ConsolidatedSup.pipeline_running?(backend)

      assert :ok = ConsolidatedSup.stop_pipeline(backend)
      refute ConsolidatedSup.pipeline_running?(backend)
    end

    test "stop_pipeline/1 returns error if pipeline not running", %{backend: backend} do
      refute ConsolidatedSup.pipeline_running?(backend)
      assert {:error, :not_found} = ConsolidatedSup.stop_pipeline(backend)
    end

    test "stop_pipeline/1 accepts backend_id integer", %{backend: backend} do
      assert {:ok, _pid} = ConsolidatedSup.start_pipeline(backend)
      assert ConsolidatedSup.pipeline_running?(backend)

      assert :ok = ConsolidatedSup.stop_pipeline(backend.id)
      refute ConsolidatedSup.pipeline_running?(backend)
    end

    test "stop_pipeline/1 with backend_id returns error if not running", %{backend: backend} do
      refute ConsolidatedSup.pipeline_running?(backend)
      assert {:error, :not_found} = ConsolidatedSup.stop_pipeline(backend.id)
    end

    test "pipeline_running?/1 returns false when pipeline not started", %{backend: backend} do
      refute ConsolidatedSup.pipeline_running?(backend)
    end

    test "count_pipelines/0 returns correct count", %{backend: backend} do
      initial_count = ConsolidatedSup.count_pipelines()

      assert {:ok, _pid} = ConsolidatedSup.start_pipeline(backend)
      assert ConsolidatedSup.count_pipelines() == initial_count + 1

      assert :ok = ConsolidatedSup.stop_pipeline(backend)
      assert ConsolidatedSup.count_pipelines() == initial_count
    end

    test "list_pipelines/0 returns backend ids and pids", %{backend: backend} do
      assert {:ok, _pid} = ConsolidatedSup.start_pipeline(backend)

      pipelines = ConsolidatedSup.list_pipelines()
      assert {id, pid} = Enum.find(pipelines, fn {id, _} -> id == backend.id end)
      assert id == backend.id
      assert is_pid(pid)
    end
  end

  describe "consolidated queue integration" do
    setup do
      insert(:plan, name: "Free")
      user = insert(:user)

      backend =
        insert(:backend,
          type: :webhook,
          user: user,
          config: %{url: "http://example.com"}
        )

      source = insert(:source, user: user, backends: [backend])

      stub(Adaptor, :get_adaptor, fn _backend -> FakeConsolidatedAdaptor end)
      stub(Adaptor, :consolidated_ingest?, fn _backend -> true end)

      IngestEventQueue.upsert_tid({:consolidated, backend.id, nil})

      on_exit(fn ->
        ConsolidatedSup.stop_pipeline(backend.id)
      end)

      [source: source, backend: backend]
    end

    test "consolidated queue accepts events", %{source: source, backend: backend} do
      events = for _ <- 1..5, do: build(:log_event, source: source)
      IngestEventQueue.add_to_table({:consolidated, backend.id}, events)

      pending_counts = IngestEventQueue.list_pending_counts({:consolidated, backend.id})
      total_pending = Enum.reduce(pending_counts, 0, fn {_key, count}, acc -> acc + count end)
      assert total_pending == 5
    end
  end

  describe "multi-source consolidated queue" do
    setup do
      insert(:plan, name: "Free")
      user = insert(:user)

      backend =
        insert(:backend,
          type: :webhook,
          user: user,
          config: %{url: "http://example.com"}
        )

      sources =
        for i <- 1..3 do
          insert(:source, name: "source_#{i}", user: user, backends: [backend])
        end

      stub(Adaptor, :get_adaptor, fn _backend -> FakeConsolidatedAdaptor end)
      stub(Adaptor, :consolidated_ingest?, fn _backend -> true end)

      IngestEventQueue.upsert_tid({:consolidated, backend.id, nil})

      on_exit(fn ->
        ConsolidatedSup.stop_pipeline(backend.id)
      end)

      [backend: backend, sources: sources, user: user]
    end

    test "events from different sources have distinct origin_source_ids", %{sources: sources} do
      events =
        Enum.flat_map(sources, fn source ->
          for _ <- 1..5, do: build(:log_event, source: source)
        end)

      origin_ids_by_source =
        events
        |> Enum.group_by(& &1.origin_source_id)
        |> Map.keys()

      assert length(origin_ids_by_source) == 3

      Enum.each(sources, fn source ->
        matching_events = Enum.filter(events, &(&1.origin_source_id == source.token))
        assert length(matching_events) == 5
      end)
    end

    test "all sources share single consolidated queue key", %{backend: backend, sources: sources} do
      Enum.each(sources, fn source ->
        event = build(:log_event, source: source)
        IngestEventQueue.add_to_table({:consolidated, backend.id}, [event])
      end)

      pending_counts = IngestEventQueue.list_pending_counts({:consolidated, backend.id})

      queue_keys =
        pending_counts
        |> Enum.map(fn {{:consolidated, bid, _ref}, _count} -> bid end)
        |> Enum.uniq()

      assert queue_keys == [backend.id]
    end
  end
end
