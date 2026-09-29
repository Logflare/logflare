defmodule Logflare.Backends.DynamicPipelineTest do
  use Logflare.DataCase

  alias Logflare.Backends.DynamicPipeline
  alias Logflare.Backends
  alias Logflare.Sources.Source.BigQuery.Pipeline
  alias Logflare.PipelinesTest.StubPipeline
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.Spool.Queue.PubSub, as: QueueMod
  alias Logflare.Backends.Spool.SpoolAck

  import ExUnit.CaptureLog

  setup do
    insert(:plan)
    user = insert(:user)
    source = insert(:source, user: user)

    backend = insert(:backend, type: :bigquery)

    # create the startup queue
    IngestEventQueue.upsert_tid({source.id, backend.id, nil})

    [
      name: Backends.via_source(source, :some_mod, backend),
      pipeline_args: [
        source: source,
        backend: backend
      ]
    ]
  end

  test "add_pipeline/1 can scale up pipelines", %{name: name, pipeline_args: pipeline_args} do
    start_supervised!(
      {DynamicPipeline,
       name: name, pipeline: Pipeline, pipeline_args: pipeline_args, max_pipelines: 1}
    )

    assert DynamicPipeline.pipeline_count(name) == 0
    assert {:ok, 1, new_name} = DynamicPipeline.add_pipeline(name)
    assert DynamicPipeline.pipeline_count(name) == 1
    assert is_tuple(new_name)
    # upper limit
    assert {:error, :max_pipelines} = DynamicPipeline.add_pipeline(name)
  end

  test "remove_pipeline/1 can scale down pipelines", %{name: name, pipeline_args: pipeline_args} do
    start_supervised!(
      {DynamicPipeline,
       name: name, pipeline: Pipeline, pipeline_args: pipeline_args, min_pipelines: 1}
    )

    assert DynamicPipeline.pipeline_count(name) == 1
    assert {:ok, 2, _} = DynamicPipeline.add_pipeline(name)
    assert {:ok, :draining, removed_id} = DynamicPipeline.remove_pipeline(name)

    # removal is asynchronous -- wait for the scheduled termination to land
    TestUtils.retry_assert(fn ->
      assert DynamicPipeline.pipeline_count(name) == 1
    end)

    assert DynamicPipeline.whereis(removed_id) == nil
    # lower limit
    assert {:error, :min_pipelines} = DynamicPipeline.remove_pipeline(name)
    assert DynamicPipeline.pipeline_count(name) == 1
  end

  test "remove_pipeline/1 respects min_pipelines against a shard still draining from a prior call",
       %{name: name, pipeline_args: pipeline_args} do
    start_supervised!(
      {DynamicPipeline,
       name: name, pipeline: Pipeline, pipeline_args: pipeline_args, min_pipelines: 1}
    )

    assert {:ok, 2, _} = DynamicPipeline.add_pipeline(name)

    assert {:ok, :draining, _first_id} = DynamicPipeline.remove_pipeline(name)

    # `pipeline_count/1` still reports 2 here -- the first shard hasn't actually
    # terminated yet -- so this call must not proceed as if a second shard were
    # still safely removable.
    assert {:error, :min_pipelines} = DynamicPipeline.remove_pipeline(name)

    TestUtils.retry_assert(fn ->
      assert DynamicPipeline.pipeline_count(name) == 1
    end)
  end

  test "remove_pipeline/1 migrates a shard's pending events to a surviving shard instead of destroying them",
       %{name: name, pipeline_args: pipeline_args} do
    start_supervised!(
      {DynamicPipeline,
       name: name, pipeline: Pipeline, pipeline_args: pipeline_args, min_pipelines: 1}
    )

    source = Keyword.fetch!(pipeline_args, :source)
    backend = Keyword.fetch!(pipeline_args, :backend)

    assert {:ok, 2, _} = DynamicPipeline.add_pipeline(name)
    assert {:ok, 3, _} = DynamicPipeline.add_pipeline(name)

    shard_producers =
      for id <- DynamicPipeline.list_pipelines(name) do
        producer_pid = id |> Broadway.producer_names() |> hd() |> GenServer.whereis()
        sid_bid_pid = {source.id, backend.id, producer_pid}
        le = build(:log_event, source: source)
        assert :ok = IngestEventQueue.add_to_table(sid_bid_pid, [le])
        assert IngestEventQueue.total_pending(sid_bid_pid) == 1
        {id, sid_bid_pid}
      end

    assert IngestEventQueue.total_pending({source.id, backend.id}) == 3

    assert {:ok, :draining, removed_id} = DynamicPipeline.remove_pipeline(name)

    {_id, removed_sid_bid_pid} =
      Enum.find(shard_producers, fn {id, _key} -> id == removed_id end)

    # removal is asynchronous -- wait for the scheduled termination to land
    TestUtils.retry_assert(fn ->
      refute Process.alive?(elem(removed_sid_bid_pid, 2))
    end)

    # The removed shard's own ETS table is gone -- destroyed along with its
    # process -- but its pending event was moved to a surviving shard first,
    # not lost.
    assert IngestEventQueue.total_pending(removed_sid_bid_pid) == {:error, :not_initialized}
    assert IngestEventQueue.total_pending({source.id, backend.id}) == 3

    surviving = for {id, sid_bid_pid} <- shard_producers, id != removed_id, do: sid_bid_pid
    assert Enum.all?(surviving, fn key -> Process.alive?(elem(key, 2)) end)
    assert surviving |> Enum.map(&IngestEventQueue.total_pending/1) |> Enum.sum() == 3
  end

  test "remove_pipeline/1 does not bump or ack SpoolAck when migrating a shard's pending events",
       %{name: name, pipeline_args: pipeline_args} do
    start_supervised!(
      {DynamicPipeline,
       name: name, pipeline: Pipeline, pipeline_args: pipeline_args, min_pipelines: 1}
    )

    source = Keyword.fetch!(pipeline_args, :source)
    backend = Keyword.fetch!(pipeline_args, :backend)

    assert {:ok, 2, _} = DynamicPipeline.add_pipeline(name)

    shards =
      for id <- DynamicPipeline.list_pipelines(name) do
        producer_pid = id |> Broadway.producer_names() |> hd() |> GenServer.whereis()
        sid_bid_pid = {source.id, backend.id, producer_pid}
        handle = "spool-handle-#{System.unique_integer([:positive])}"
        SpoolAck.register(handle, QueueMod, "queue-url")
        le = %{build(:log_event, source: source) | spool_handle: handle}
        assert :ok = IngestEventQueue.add_to_table(sid_bid_pid, [le])
        {id, sid_bid_pid, handle}
      end

    assert {:ok, :draining, removed_id} = DynamicPipeline.remove_pipeline(name)

    {_id, removed_key, removed_handle} =
      Enum.find(shards, fn {id, _key, _handle} -> id == removed_id end)

    {_id, surviving_key, surviving_handle} =
      Enum.find(shards, fn {id, _key, _handle} -> id != removed_id end)

    # removal is asynchronous -- wait for the scheduled termination to land
    TestUtils.retry_assert(fn ->
      refute Process.alive?(elem(removed_key, 2))
    end)

    # The migration itself must be invisible to SpoolAck -- no extra bump, no
    # premature ack -- for both the migrated pointer and the one already on
    # the surviving shard.
    assert [{^removed_handle, 1, QueueMod, "queue-url", _registered_at}] =
             :ets.lookup(:spool_ack, removed_handle)

    assert [{^surviving_handle, 1, QueueMod, "queue-url", _registered_at}] =
             :ets.lookup(:spool_ack, surviving_handle)

    assert IngestEventQueue.total_pending(surviving_key) == 2
  end

  test "remove_pipeline/1 migrates to a live survivor even when a stale mapper entry is also a candidate",
       %{name: name, pipeline_args: pipeline_args} do
    start_supervised!(
      {DynamicPipeline,
       name: name, pipeline: Pipeline, pipeline_args: pipeline_args, min_pipelines: 1}
    )

    source = Keyword.fetch!(pipeline_args, :source)
    backend = Keyword.fetch!(pipeline_args, :backend)

    assert {:ok, 2, _} = DynamicPipeline.add_pipeline(name)

    shard_producers =
      for id <- DynamicPipeline.list_pipelines(name) do
        producer_pid = id |> Broadway.producer_names() |> hd() |> GenServer.whereis()
        sid_bid_pid = {source.id, backend.id, producer_pid}
        le = build(:log_event, source: source)
        assert :ok = IngestEventQueue.add_to_table(sid_bid_pid, [le])
        {id, sid_bid_pid}
      end

    test_pid = self()

    stale_pid =
      spawn(fn ->
        table_key = {source.id, backend.id, self()}
        {:ok, _tid} = IngestEventQueue.upsert_tid(table_key)
        send(test_pid, :ready)
        Process.sleep(:infinity)
      end)

    stale_ref = Process.monitor(stale_pid)
    assert_receive :ready
    Process.exit(stale_pid, :kill)
    assert_receive {:DOWN, ^stale_ref, :process, ^stale_pid, :killed}

    assert IngestEventQueue.total_pending({source.id, backend.id}) == 2

    assert {:ok, :draining, removed_id} = DynamicPipeline.remove_pipeline(name)

    {_id, removed_sid_bid_pid} =
      Enum.find(shard_producers, fn {id, _key} -> id == removed_id end)

    TestUtils.retry_assert(fn ->
      refute Process.alive?(elem(removed_sid_bid_pid, 2))
    end)

    assert IngestEventQueue.total_pending({source.id, backend.id}) == 2

    {_id, surviving_sid_bid_pid} =
      Enum.find(shard_producers, fn {id, _key} -> id != removed_id end)

    assert IngestEventQueue.total_pending(surviving_sid_bid_pid) == 2
  end

  test ":initial_count will determine number of pipelines at the start",
       %{name: name, pipeline_args: pipeline_args} do
    start_supervised!(
      {DynamicPipeline,
       name: name,
       pipeline: Pipeline,
       pipeline_args: pipeline_args,
       initial_count: 5,
       max_pipelines: 11,
       resolve_count: fn _state ->
         6
       end,
       resolve_interval: 100}
    )

    TestUtils.retry_assert(fn ->
      assert DynamicPipeline.pipeline_count(name) == 5
    end)

    TestUtils.retry_assert(fn ->
      assert DynamicPipeline.pipeline_count(name) == 6
    end)
  end

  test ":resolve_count and :resolve_interval option will determine number of pipelines to start periodically",
       %{name: name, pipeline_args: pipeline_args} do
    pid = spawn(&TestUtils.wait_for_stop/0)

    start_supervised!(
      {DynamicPipeline,
       name: name,
       pipeline: Pipeline,
       pipeline_args: pipeline_args,
       max_pipelines: 11,
       resolve_count: fn state ->
         assert is_map_key(state, :last_count_increase)
         assert is_map_key(state, :last_count_decrease)

         if Process.alive?(pid) do
           5
         else
           10
         end
       end,
       resolve_interval: 100}
    )

    TestUtils.retry_assert(fn ->
      assert DynamicPipeline.pipeline_count(name) == 0
    end)

    TestUtils.retry_assert(fn ->
      assert DynamicPipeline.pipeline_count(name) == 5
    end)

    ref = Process.monitor(pid)
    send(pid, :stop)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}

    TestUtils.retry_assert(fn ->
      assert DynamicPipeline.pipeline_count(name) == 10
    end)
  end

  test "error in resolve_count does not crash everything",
       %{name: name, pipeline_args: pipeline_args} do
    assert capture_log(fn ->
             pid =
               start_supervised!(
                 {DynamicPipeline,
                  name: name,
                  pipeline: Pipeline,
                  pipeline_args: pipeline_args,
                  resolve_count: fn _state ->
                    raise "some error"
                  end,
                  resolve_interval: 100}
               )

             coordinator = DynamicPipeline.find_coordinator_name(name)
             TestUtils.send_and_wait_for_handling(coordinator, :check)
             assert Process.alive?(pid)
           end) =~ "some error"
  end

  test "pulls events from startup queue with bigquery pipeline", %{
    name: name,
    pipeline_args: pipeline_args
  } do
    pid = self()
    ref = make_ref()

    Logflare.Google.BigQuery
    |> expect(:stream_batch!, fn _, _ ->
      send(pid, ref)
      {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
    end)

    source = pipeline_args[:source]
    backend = pipeline_args[:backend]

    le = build(:log_event, source: source)
    IngestEventQueue.upsert_tid({source.id, backend.id, nil})
    IngestEventQueue.add_to_table({source.id, backend.id, nil}, [le])

    start_supervised!(
      {DynamicPipeline,
       name: name, pipeline: Pipeline, pipeline_args: pipeline_args, min_pipelines: 1}
    )

    assert_receive ^ref, 2_000
  end

  test "whereis/1", %{name: name, pipeline_args: pipeline_args} do
    pid =
      start_link_supervised!(
        {DynamicPipeline, name: name, pipeline: StubPipeline, pipeline_args: pipeline_args}
      )

    assert DynamicPipeline.whereis(name) == pid
  end

  test "trace increases and decreases of pipelines",
       %{name: name, pipeline_args: pipeline_args} do
    ref_increment =
      :telemetry_test.attach_event_handlers(self(), [
        [:logflare, :backends, :dynamic_pipeline, :increment]
      ])

    ref_decrement =
      :telemetry_test.attach_event_handlers(self(), [
        [:logflare, :backends, :dynamic_pipeline, :decrement]
      ])

    pid = spawn(&TestUtils.wait_for_stop/0)

    start_supervised!(
      {DynamicPipeline,
       name: name,
       pipeline: Pipeline,
       pipeline_args: pipeline_args,
       max_pipelines: 11,
       resolve_count: fn state ->
         assert is_map_key(state, :last_count_increase)
         assert is_map_key(state, :last_count_decrease)

         if Process.alive?(pid) do
           10
         else
           5
         end
       end,
       resolve_interval: 100}
    )

    TestUtils.retry_assert(fn ->
      assert DynamicPipeline.pipeline_count(name) == 0
    end)

    TestUtils.retry_assert(fn ->
      assert DynamicPipeline.pipeline_count(name) == 10
    end)

    ref = Process.monitor(pid)
    send(pid, :stop)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}

    TestUtils.retry_assert(fn ->
      assert DynamicPipeline.pipeline_count(name) == 5
    end)

    source_id = pipeline_args[:source].id
    source_token = pipeline_args[:source].token
    backend_id = pipeline_args[:backend].id
    backend_token = pipeline_args[:backend].token
    backend_type = pipeline_args[:backend].type

    assert_received {[:logflare, :backends, :dynamic_pipeline, :increment], ^ref_increment,
                     %{error_count: 0, success_count: 10, from_pipeline_count: 0},
                     %{
                       source_id: ^source_id,
                       source_token: ^source_token,
                       backend_id: ^backend_id,
                       backend_token: ^backend_token,
                       backend_type: ^backend_type
                     }}

    assert_received {[:logflare, :backends, :dynamic_pipeline, :decrement], ^ref_decrement,
                     %{error_count: 0, success_count: 5, from_pipeline_count: 10},
                     %{
                       source_id: ^source_id,
                       source_token: ^source_token,
                       backend_id: ^backend_id,
                       backend_token: ^backend_token,
                       backend_type: ^backend_type
                     }}
  end
end
