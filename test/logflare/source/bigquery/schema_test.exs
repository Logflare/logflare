defmodule Logflare.Sources.Source.BigQuery.SchemaTest do
  @moduledoc false
  use Logflare.DataCase

  alias Logflare.Sources.Source.BigQuery.Schema
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

  test "updates correctly" do
    # The suite allows 900,000 updates per minute, so both updates could patch BigQuery.
    # Use one update per minute here to throttle the second, and restore the config on exit.
    schema_config = Application.fetch_env!(:logflare, Schema)
    on_exit(fn -> Application.put_env(:logflare, Schema, schema_config) end)
    Application.put_env(:logflare, Schema, Keyword.put(schema_config, :updates_per_minute, 1))

    user = insert(:user)
    source = insert(:source, user: user)
    schema = TestUtils.default_bq_schema()

    insert(:source_schema,
      source: source,
      source_id: source.id,
      bigquery_schema: schema,
      schema_flat_map: SchemaUtils.bq_schema_to_flat_typemap(schema)
    )

    GoogleApi.BigQuery.V2.Api.Tables
    |> expect(:bigquery_tables_patch, 1, fn _conn,
                                            _project_id,
                                            _dataset_id,
                                            _table_name,
                                            [body: body] ->
      schema = body.schema

      assert %_{name: "test", type: "INTEGER"} =
               TestUtils.get_bq_field_schema(schema, "metadata.test")

      {:ok, %{}}
    end)

    Logflare.Mailer
    |> expect(:deliver, 1, fn _ -> :ok end)

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

    le = build(:log_event, source: source, metadata: %{"change" => 123})
    assert :ok = Schema.update(pid, le, source)

    # Wait for both casts; Mimic verifies the single patch expectation on test exit.
    :sys.get_state(pid)
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
