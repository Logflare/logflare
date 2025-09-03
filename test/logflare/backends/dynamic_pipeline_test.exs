defmodule Logflare.Backends.DynamicPipelineTest do
  use Logflare.DataCase

  alias Logflare.Backends.DynamicPipeline
  alias Logflare.Backends
  alias Logflare.Sources.Source.BigQuery.Pipeline
  alias Logflare.PipelinesTest.StubPipeline
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.IngestEventQueue

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
    assert {:ok, 1, removed_id} = DynamicPipeline.remove_pipeline(name)
    # lower limit
    assert {:error, :min_pipelines} = DynamicPipeline.remove_pipeline(name)
    assert DynamicPipeline.pipeline_count(name) == 1
    assert DynamicPipeline.whereis(removed_id) == nil
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
    pid =
      spawn(fn ->
        :timer.sleep(400)
      end)

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

             :timer.sleep(300)
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

    pid =
      spawn(fn ->
        :timer.sleep(400)
      end)

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
