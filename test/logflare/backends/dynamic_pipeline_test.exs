defmodule Logflare.DynamicPipelineTest do
  use Logflare.DataCase

  alias Logflare.Backends.DynamicPipeline
  alias Logflare.Backends
  alias Logflare.Source.BigQuery.Pipeline
  alias Logflare.PipelinesTest.StubPipeline

  setup do
    user = insert(:user)
    source = insert(:source, user: user)

    [
      name: Backends.via_source(source.id, :some_mod, nil),
      pipeline_args: [
        source: source,
        backend_id: nil,
        backend_token: nil
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

  test "remove_pipeline/1 can scake down pipelines", %{name: name, pipeline_args: pipeline_args} do
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

  test "buffer_len/1", %{name: name, pipeline_args: pipeline_args} do
    start_supervised!(
      {DynamicPipeline, name: name, pipeline: Pipeline, pipeline_args: pipeline_args}
    )

    assert DynamicPipeline.buffer_len(name) == 0
  end

  test "push_messages/2 with bigquery pipeline", %{name: name, pipeline_args: pipeline_args} do
    pid = self()
    ref = make_ref()

    Logflare.Google.BigQuery
    |> expect(:stream_batch!, fn _, _ ->
      send(pid, ref)
      {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
    end)

    start_supervised!(
      {DynamicPipeline, name: name, pipeline: Pipeline, pipeline_args: pipeline_args}
    )

    message = %Broadway.Message{
      data: build(:log_event),
      acknowledger: {DynamicPipeline, nil, nil}
    }

    assert :ok = DynamicPipeline.push_messages(name, [message])
    assert_receive ^ref, 2_000
  end

  test "push_messages/2 will scale up to max_shards and then push back if completely full", %{
    name: name,
    pipeline_args: pipeline_args
  } do
    start_supervised!(
      {DynamicPipeline,
       name: name,
       pipeline: StubPipeline,
       pipeline_args: pipeline_args,
       max_buffer_len: 100,
       max_pipelines: 2,
       monitor_interval: 50}
    )

    message = %Broadway.Message{
      data: build(:log_event),
      acknowledger: {DynamicPipeline, nil, nil}
    }

    DynamicPipeline.push_messages(name, List.duplicate(message, 500))
    DynamicPipeline.push_messages(name, List.duplicate(message, 500))
    assert DynamicPipeline.healthy?(name) == false
    # insert all into pipeline
    # allows for overshoot of the max_buffer_len if the configured producer's buffer is larger
    assert DynamicPipeline.buffer_len(name) >= 100

    assert {:error, :buffer_full} =
             DynamicPipeline.push_messages(name, List.duplicate(message, 1000))
  end

  test "healthy?/1 on startup", %{name: name, pipeline_args: pipeline_args} do
    refute DynamicPipeline.healthy?(name)

    start_link_supervised!(
      {DynamicPipeline, name: name, pipeline: StubPipeline, pipeline_args: pipeline_args}
    )

    :timer.sleep(200)
    assert DynamicPipeline.healthy?(name)
  end

  test "whereis/1", %{name: name, pipeline_args: pipeline_args} do
    refute DynamicPipeline.healthy?(name)

    pid =
      start_link_supervised!(
        {DynamicPipeline, name: name, pipeline: StubPipeline, pipeline_args: pipeline_args}
      )

    assert DynamicPipeline.whereis(name) == pid
  end

  test "auto-terminate pipelines after idle_shutdown_after", %{
    name: name,
    pipeline_args: pipeline_args
  } do
    start_supervised!(
      {DynamicPipeline,
       name: name,
       pipeline: StubPipeline,
       pipeline_args: pipeline_args,
       idle_shutdown_after: 150,
       min_pipelines: 1}
    )

    assert {:ok, 2, _} = DynamicPipeline.add_pipeline(name)
    :timer.sleep(400)
    assert DynamicPipeline.pipeline_count(name) == 1
  end

  test "do not auto-terminate pipeline if touched", %{
    name: name,
    pipeline_args: pipeline_args
  } do
    start_supervised!(
      {DynamicPipeline,
       name: name,
       pipeline: StubPipeline,
       pipeline_args: pipeline_args,
       idle_shutdown_after: 400,
       min_pipelines: 1}
    )

    assert {:ok, 2, _} = DynamicPipeline.add_pipeline(name)

    for _i <- 0..3 do
      :timer.sleep(300)

      for pipeline_name <- DynamicPipeline.list_pipelines(name) do
        DynamicPipeline.touch_pipeline(pipeline_name)
      end
    end

    assert DynamicPipeline.pipeline_count(name) == 2
  end

  test "auto-terminate min pipelines after min_idle_shutdown_after", %{
    name: name,
    pipeline_args: pipeline_args
  } do
    start_supervised!(
      {DynamicPipeline,
       name: name,
       pipeline: StubPipeline,
       pipeline_args: pipeline_args,
       idle_shutdown_after: 150,
       min_idle_shutdown_after: 300,
       min_pipelines: 1}
    )

    assert DynamicPipeline.pipeline_count(name) == 1
    :timer.sleep(1_000)
    assert DynamicPipeline.pipeline_count(name) == 0
  end

  test "does not auto-terminate min pipelines if touched", %{
    name: name,
    pipeline_args: pipeline_args
  } do
    start_supervised!(
      {DynamicPipeline,
       name: name,
       pipeline: StubPipeline,
       pipeline_args: pipeline_args,
       idle_shutdown_after: 150,
       min_idle_shutdown_after: 500,
       min_pipelines: 1}
    )

    assert DynamicPipeline.pipeline_count(name) == 1

    for _i <- 0..3 do
      :timer.sleep(300)

      for pipeline_name <- DynamicPipeline.list_pipelines(name) do
        DynamicPipeline.touch_pipeline(pipeline_name)
      end
    end

    :timer.sleep(200)
    assert DynamicPipeline.pipeline_count(name) == 1
  end
end
