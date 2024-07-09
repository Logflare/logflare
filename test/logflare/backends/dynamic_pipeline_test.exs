defmodule Logflare.DynamicPipelineTest do
  use Logflare.DataCase

  alias Logflare.Backends.DynamicPipeline
  alias Logflare.Backends
  alias Logflare.Source.BigQuery.Pipeline
  alias Logflare.PipelinesTest.StubPipeline
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.IngestEventQueue

  setup do
    user = insert(:user)
    source = insert(:source, user: user)

    backend = insert(:backend, type: :bigquery)
    IngestEventQueue.upsert_tid({source, backend})
    start_supervised!({IngestEventQueue.DemandWorker, source: source, backend: backend})

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

  test ":resolve_count and :resolve_interval option will determine number of pipelines to start periodically",
       %{name: name, pipeline_args: pipeline_args} do
    pid =
      spawn(fn ->
        :timer.sleep(300)
      end)

    start_supervised!(
      {DynamicPipeline,
       name: name,
       pipeline: Pipeline,
       pipeline_args: pipeline_args,
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

    assert DynamicPipeline.pipeline_count(name) == 5
    :timer.sleep(600)
    assert DynamicPipeline.pipeline_count(name) == 10
  end

  test "pulls events from queue with  bigquery pipeline", %{
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
    IngestEventQueue.add_to_table({source, backend}, [le])

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
end
