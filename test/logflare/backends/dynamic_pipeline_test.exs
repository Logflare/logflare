defmodule Logflare.DynamicPipelineTest do
  use Logflare.DataCase

  alias Logflare.Backends.DynamicPipeline
  alias Logflare.Backends
  alias Logflare.Source.BigQuery.Pipeline

  setup do
    user = insert(:user)
    source = insert(:source, user: user)

    [
      name: Backends.via_source(source.id, :some_mod, nil),
      pipeline_args: [
        source: source
      ]
    ]
  end

  test "can scale a given pipeline", %{name: name, pipeline_args: pipeline_args} do
    pid =
      start_supervised!(
        {DynamicPipeline, name: name, pipeline: Pipeline, pipeline_args: pipeline_args}
      )

    assert DynamicPipeline.shard_count(name) == 1
    assert {:ok, 2} = DynamicPipeline.add_shard(name)
  end

  test "buffer_len/1", %{name: name, pipeline_args: pipeline_args} do
    pid =
      start_supervised!(
        {DynamicPipeline, name: name, pipeline: Pipeline, pipeline_args: pipeline_args}
      )

    assert DynamicPipeline.buffer_len(name) == 0
  end

  test "push/2", %{name: name} do
    pid = self()
    ref = make_ref()

    Logflare.Google.BigQuery
    |> expect(:stream_batch!, fn _, _ ->
      send(pid, ref)
      {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
    end)

    le = build(:log_event)
    assert :ok = DynamicPipeline.push(name, [le])
    assert_receive ^ref
  end
end
