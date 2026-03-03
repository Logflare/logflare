defmodule Logflare.Backends.ConsolidatedSupTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.ConsolidatedSup
  alias Logflare.Backends.IngestEventQueue

  describe "ConsolidatedSup" do
    setup do
      insert(:plan, name: "Free")

      {_source, backend, cleanup_fn} = setup_clickhouse_test()
      on_exit(cleanup_fn)

      [backend: backend]
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

      {source, backend, cleanup_fn} = setup_clickhouse_test()
      on_exit(cleanup_fn)

      IngestEventQueue.upsert_tid({:consolidated, backend.id, nil})

      [source: source, backend: backend]
    end

    test "pipeline processes events from consolidated queue", %{
      source: source,
      backend: backend
    } do
      assert {:ok, _pid} = ConsolidatedSup.start_pipeline(backend)

      events = for _ <- 1..5, do: build(:log_event, source: source)
      IngestEventQueue.add_to_table({:consolidated, backend.id}, events)

      TestUtils.retry_assert(fn ->
        pending_counts = IngestEventQueue.list_pending_counts({:consolidated, backend.id})
        total_pending = Enum.reduce(pending_counts, 0, fn {_key, count}, acc -> acc + count end)
        assert total_pending < 5
      end)
    end
  end

  describe "multi-source consolidated ingestion" do
    setup do
      insert(:plan, name: "Free")
      user = insert(:user)

      {:ok, backend} =
        Logflare.Backends.create_backend(%{
          type: :clickhouse,
          user_id: user.id,
          name: "Multi-Source Test Backend",
          config: %{
            url: "http://localhost",
            port: 8123,
            database: "test_db",
            username: "user",
            password: "pass"
          }
        })

      sources =
        for i <- 1..10 do
          insert(:source, name: "source_#{i}", user: user, backends: [backend])
        end

      IngestEventQueue.upsert_tid({:consolidated, backend.id, nil})

      on_exit(fn ->
        try do
          ConsolidatedSup.stop_pipeline(backend.id)
        catch
          _kind, value -> value
        end
      end)

      [backend: backend, sources: sources, user: user]
    end

    test "consolidates events from 10 sources into single pipeline", %{
      backend: backend,
      sources: sources
    } do
      assert ConsolidatedSup.pipeline_running?(backend)

      events_per_source = 50
      total_events = length(sources) * events_per_source

      all_events =
        Enum.flat_map(sources, fn source ->
          for _ <- 1..events_per_source, do: build(:log_event, source: source)
        end)

      assert length(all_events) == total_events

      source_ids = all_events |> Enum.map(& &1.source_uuid) |> Enum.uniq()
      assert length(source_ids) == 10

      IngestEventQueue.add_to_table({:consolidated, backend.id}, all_events)

      TestUtils.retry_assert(fn ->
        pending_counts = IngestEventQueue.list_pending_counts({:consolidated, backend.id})
        total_pending = Enum.reduce(pending_counts, 0, fn {_key, count}, acc -> acc + count end)
        assert total_pending < total_events
      end)
    end

    test "events from different sources have distinct source_uuids", %{
      sources: sources
    } do
      events =
        Enum.flat_map(sources, fn source ->
          for _ <- 1..20, do: build(:log_event, source: source)
        end)

      origin_ids_by_source =
        events
        |> Enum.group_by(& &1.source_uuid)
        |> Map.keys()

      assert length(origin_ids_by_source) == 10

      Enum.each(sources, fn source ->
        matching_events = Enum.filter(events, &(&1.source_uuid == source.token))
        assert length(matching_events) == 20
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
