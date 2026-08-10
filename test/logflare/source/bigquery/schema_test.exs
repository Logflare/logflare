defmodule Logflare.Sources.Source.BigQuery.SchemaTest do
  @moduledoc false
  use Logflare.DataCase

  alias Logflare.Sources.Source.BigQuery.Schema
  alias Logflare.Sources.Source.BigQuery.SchemaBuilder
  alias Logflare.Google.BigQuery.SchemaUtils

  setup do
    insert(:plan)
    :ok
  end

  test "next_update_ts/1" do
    next_update = Schema.next_update_ts(6) |> trunc()
    assert String.length("#{next_update}") == String.length("#{System.system_time(:millisecond)}")
    seconds = (next_update - System.system_time(:millisecond)) / 1000
    assert seconds <= 10
    assert seconds > 9
  end

  test "plan_update/3 skips schema building during the update cooldown" do
    schema =
      %{}
      |> SchemaBuilder.build_table_schema(SchemaBuilder.initial_table_schema())

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        assert :noop =
                 Schema.plan_update(%{"invalid" => []}, schema, %{
                   next_update: System.system_time(:millisecond) + 60_000
                 })
      end)

    refute log =~ "Field schema type change error"
  end

  test "plan_update/3 compares schemas without changing update behavior" do
    schema =
      %{}
      |> SchemaBuilder.build_table_schema(SchemaBuilder.initial_table_schema())

    state = %{next_update: 0}

    assert :noop = Schema.plan_update(%{}, schema, state)
    assert {:update, updated_schema} = Schema.plan_update(%{"new_field" => 1}, schema, state)

    assert %_{name: "new_field", type: "INTEGER"} =
             TestUtils.get_bq_field_schema(updated_schema, "new_field")
  end

  test "reserve_update_slot/2 caps concurrent pending updates" do
    counter = :atomics.new(1, [])

    assert {:ok, _reservation} = Schema.reserve_update_slot(counter, 2)
    assert {:ok, _reservation} = Schema.reserve_update_slot(counter, 2)
    assert :full = Schema.reserve_update_slot(counter, 2)
    assert Schema.pending_update_slots(counter) == 2
  end

  test "stale reservations cannot decrement a restarted Schema generation" do
    counter = :atomics.new(1, [])
    :ok = Schema.reset_update_slots(counter)
    assert {:ok, {^counter, stale_generation}} = Schema.reserve_update_slot(counter, 2)

    :ok = Schema.reset_update_slots(counter)
    assert {:ok, {^counter, current_generation}} = Schema.reserve_update_slot(counter, 2)
    assert current_generation > stale_generation

    assert :ok = Schema.release_update_slot(counter, stale_generation)
    assert Schema.pending_update_slots(counter) == 1
    assert :ok = Schema.release_update_slot(counter, current_generation)
    assert Schema.pending_update_slots(counter) == 0
  end

  test "reserved updates release their slot when handling starts" do
    user = insert(:user)
    source = insert(:source, user: user, lock_schema: true)
    counter = :atomics.new(1, [])

    pid =
      start_supervised!(
        {Schema,
         [
           source: source,
           sample_counter: counter,
           plan: %{limit_source_fields_limit: 500},
           bigquery_project_id: "some-id",
           bigquery_dataset_id: "some-id"
         ]}
      )

    assert {:ok, reservation} = Schema.reserve_update_slot(counter, 1)
    assert :ok = Schema.update(pid, build(:log_event, source: source), source, reservation)
    :sys.get_state(pid)

    assert Schema.pending_update_slots(counter) == 0
    assert {:ok, _reservation} = Schema.reserve_update_slot(counter, 1)
  end

  test "updates correctly" do
    user = insert(:user)
    source = insert(:source, user: user)
    schema = TestUtils.default_bq_schema()

    insert(:source_schema,
      source: source,
      source_id: source.id,
      bigquery_schema: schema,
      schema_flat_map: SchemaUtils.bq_schema_to_flat_typemap(schema)
    )

    test_pid = self()

    GoogleApi.BigQuery.V2.Api.Tables
    |> expect(:bigquery_tables_patch, 1, fn _conn,
                                            _project_id,
                                            _dataset_id,
                                            _table_name,
                                            [body: body] ->
      schema = body.schema

      assert %_{name: "test", type: "INTEGER"} =
               TestUtils.get_bq_field_schema(schema, "metadata.test")

      send(test_pid, :ok)
      {:ok, %{}}
    end)

    Logflare.Mailer
    |> expect(:deliver, 1, fn _ -> :ok end)

    handler_id = "schema-operation-#{System.unique_integer()}"

    :telemetry.attach(
      handler_id,
      [:logflare, :bigquery, :schema, :operation, :stop],
      fn _event, _measurements, metadata, pid -> send(pid, {:schema_phase, metadata}) end,
      self()
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    pid =
      start_supervised!(
        {Schema,
         [
           source: source,
           plan: %{limit_source_fields_limit: 500},
           bigquery_project_id: "some-id",
           bigquery_dataset_id: "some-id"
         ]}
      )

    le = build(:log_event, source: source, metadata: %{"test" => 123})
    assert :ok = Schema.update(pid, le, source)

    TestUtils.retry_assert(fn ->
      assert_received :ok
    end)

    :sys.get_state(pid)
    assert_receive {:schema_phase, %{phase: :patch, result: :ok}}
    assert_receive {:schema_phase, %{phase: :persist}}
    assert_receive {:schema_phase, %{phase: :notify, result: :ok}}
  end

  test "default notifications config" do
    Logflare.Mailer
    |> expect(:deliver, 1, fn _ -> :ok end)

    user = insert(:user)
    source = insert(:source, user: user)
    old_schema = TestUtils.default_bq_schema()
    new_schema = TestUtils.build_bq_schema(%{"test" => "value"})
    Schema.notify_maybe(source.token, new_schema, old_schema)
  end

  test "disabled schema notifications" do
    reject(&Logflare.Mailer.deliver/1)
    user = insert(:user)

    source =
      insert(:source, user: user, notifications: %{user_schema_update_notifications: false})

    old_schema = TestUtils.default_bq_schema()
    new_schema = TestUtils.build_bq_schema(%{"test" => "value"})
    Schema.notify_maybe(source.token, new_schema, old_schema)
  end

  test "no team user - invalid or deleted id" do
    reject(&Logflare.Mailer.deliver/1)
    user = insert(:user)

    source =
      insert(:source,
        user: user,
        notifications: %{
          user_schema_update_notifications: false,
          team_user_ids_for_schema_updates: ["123"]
        }
      )

    old_schema = TestUtils.default_bq_schema()
    new_schema = TestUtils.build_bq_schema(%{"test" => "value"})
    Schema.notify_maybe(source.token, new_schema, old_schema)
  end
end
