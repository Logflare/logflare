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

  test "restarts Schema with fresh admission state without restarting pipelines" do
    user = insert(:user)
    source = insert(:source, user: user)
    start_supervised!({SourceSup, source})

    schema_name = Backends.via_source(source, Schema, nil)
    pipeline_name = Backends.via_source(source, Pipeline, nil)
    {:via, Registry, {registry, key}} = schema_name

    assert [{schema_pid, {:schema_admission, counter, limit}}] = Registry.lookup(registry, key)
    pipeline_pid = GenServer.whereis(pipeline_name)

    assert is_pid(pipeline_pid)
    assert :ok = Schema.reserve_update_slot(counter, limit)

    Process.exit(schema_pid, :kill)

    {restarted_schema_pid, restarted_counter} =
      TestUtils.retry_assert(fn ->
        assert [{restarted_schema_pid, {:schema_admission, restarted_counter, ^limit}}] =
                 Registry.lookup(registry, key)

        assert is_pid(restarted_schema_pid)
        refute restarted_schema_pid == schema_pid
        refute restarted_counter == counter
        assert GenServer.whereis(pipeline_name) == pipeline_pid

        {restarted_schema_pid, restarted_counter}
      end)

    # A producer that reserved immediately before the crash can only cast to the
    # stale pid/counter pair; it cannot consume capacity from the replacement.
    event = build(:log_event, source: source)
    assert :ok = GenServer.cast(schema_pid, {:update, event, source, counter})
    :sys.get_state(restarted_schema_pid)

    assert Schema.pending_update_slots(counter) == 1
    assert Schema.pending_update_slots(restarted_counter) == 0
  end
end
