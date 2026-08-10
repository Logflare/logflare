defmodule Logflare.Backends.Adaptor.BigQueryAdaptorSupervisionTest do
  @moduledoc false
  use Logflare.DataCase

  alias Logflare.Backends
  alias Logflare.Backends.SourceSup
  alias Logflare.Sources.Source.BigQuery.Pipeline
  alias Logflare.Sources.Source.BigQuery.Schema

  setup do
    insert(:plan)
    :ok
  end

  test "restarts pipeline shards after their Schema admission generation restarts" do
    user = insert(:user)
    source = insert(:source, user: user)
    start_supervised!({SourceSup, source})

    schema_name = Backends.via_source(source, Schema, nil)
    pipeline_name = Backends.via_source(source, Pipeline, nil)
    schema_pid = GenServer.whereis(schema_name)
    pipeline_pid = GenServer.whereis(pipeline_name)

    assert is_pid(schema_pid)
    assert is_pid(pipeline_pid)

    Process.exit(schema_pid, :kill)

    TestUtils.retry_assert(fn ->
      restarted_schema_pid = GenServer.whereis(schema_name)
      restarted_pipeline_pid = GenServer.whereis(pipeline_name)

      assert is_pid(restarted_schema_pid)
      assert is_pid(restarted_pipeline_pid)
      refute restarted_schema_pid == schema_pid
      refute restarted_pipeline_pid == pipeline_pid
    end)
  end
end
