defmodule Logflare.Sources.Source.BigQuery.SchemaTest do
  @moduledoc false
  use Logflare.DataCase
  use ExUnitProperties

  alias GoogleApi.BigQuery.V2.Api.Tables, as: BigQueryTables
  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.SourceSchemas
  alias Logflare.Sources.Source.BigQuery.Schema

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

    BigQueryTables
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
    |> expect(:deliver, 1, fn _ ->
      send(test_pid, :schema_update_notification_delivered)
      :ok
    end)

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

    assert_receive :schema_update_notification_delivered, to_timeout(second: 30)

    # subsequent updates do not increase mock count
    le = build(:log_event, source: source, metadata: %{"change" => 123})
    assert :ok = Schema.update(pid, le, source)
  end

  test "persists schema flat map for nested and repeated fields" do
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

    BigQueryTables
    |> expect(:bigquery_tables_patch, 1, fn _conn,
                                            _project_id,
                                            _dataset_id,
                                            _table_name,
                                            [body: body] ->
      schema = body.schema

      assert %_{name: "test", type: "INTEGER"} =
               TestUtils.get_bq_field_schema(schema, "metadata.test")

      assert %_{name: "tags", mode: "REPEATED", type: "STRING"} =
               TestUtils.get_bq_field_schema(schema, "metadata.tags")

      assert %_{name: "items", mode: "REPEATED", type: "RECORD"} =
               TestUtils.get_bq_field_schema(schema, "metadata.items")

      assert %_{name: "a", type: "INTEGER"} =
               TestUtils.get_bq_field_schema(schema, "metadata.items.a")

      assert %_{name: "b", type: "INTEGER"} =
               TestUtils.get_bq_field_schema(schema, "metadata.items.b")

      {:ok, %{}}
    end)

    Logflare.Mailer
    |> expect(:deliver, 1, fn _ ->
      send(test_pid, :schema_flat_map_update_notification_delivered)
      :ok
    end)

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

    le =
      build(:log_event,
        source: source,
        metadata: %{
          "test" => 123,
          "tags" => ["a", "b"],
          "items" => [%{"a" => 1}, %{"b" => 2}]
        }
      )

    assert :ok = Schema.update(pid, le, source)

    assert_receive :schema_flat_map_update_notification_delivered, to_timeout(second: 30)

    source_schema = SourceSchemas.get_source_schema_by(source_id: source.id)

    assert source_schema.schema_flat_map == %{
             "event_message" => :string,
             "id" => :string,
             "metadata" => :map,
             "metadata.items" => :map,
             "metadata.items.a" => :integer,
             "metadata.items.b" => :integer,
             "metadata.tags" => {:list, :string},
             "metadata.test" => :integer,
             "timestamp" => :datetime
           }
  end

  property "generated metadata schemas include every field in the flat map" do
    test_pid = self()

    BigQueryTables
    |> stub(:bigquery_tables_patch, fn _conn, _project_id, _dataset_id, _table_name, _opts ->
      {:ok, %{}}
    end)

    Logflare.Mailer
    |> stub(:deliver, fn _ ->
      send(test_pid, :generated_schema_update_notification_delivered)
      :ok
    end)

    check all(metadata <- metadata_generator(), max_runs: 50) do
      user = insert(:user)
      source = insert(:source, user: user)
      schema = TestUtils.default_bq_schema()

      insert(:source_schema,
        source: source,
        source_id: source.id,
        bigquery_schema: schema,
        schema_flat_map: SchemaUtils.bq_schema_to_flat_typemap(schema)
      )

      pid =
        start_supervised!(
          {Schema,
           [
             source: source,
             plan: %{limit_source_fields_limit: 500},
             bigquery_project_id: "some-id",
             bigquery_dataset_id: "some-id"
           ]},
          id: {:schema, source.id}
        )

      le = build(:log_event, source: source, metadata: metadata)
      assert :ok = Schema.update(pid, le, source)
      assert_receive :generated_schema_update_notification_delivered, to_timeout(second: 30)

      source_schema = SourceSchemas.get_source_schema_by(source_id: source.id)
      expected_flat_map = expected_flat_map(metadata, "metadata")

      assert Map.take(source_schema.schema_flat_map, Map.keys(expected_flat_map)) ==
               expected_flat_map
    end
  end

  test "does not patch when the incoming event matches the existing schema" do
    user = insert(:user)
    source = insert(:source, user: user)
    schema = TestUtils.build_bq_schema(%{"metadata" => %{"test" => 123}})

    insert(:source_schema,
      source: source,
      source_id: source.id,
      bigquery_schema: schema,
      schema_flat_map: SchemaUtils.bq_schema_to_flat_typemap(schema)
    )

    reject(&BigQueryTables.bigquery_tables_patch/5)
    reject(&Logflare.Mailer.deliver/1)

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

  defp metadata_generator(depth \\ 2) do
    StreamData.map_of(key_generator(), metadata_value_generator(depth),
      min_length: 1,
      max_length: 4
    )
  end

  defp metadata_value_generator(0) do
    StreamData.one_of([
      scalar_generator(),
      scalar_list_generator()
    ])
  end

  defp metadata_value_generator(depth) do
    StreamData.one_of([
      scalar_generator(),
      scalar_list_generator(),
      metadata_generator(depth - 1),
      StreamData.list_of(metadata_generator(depth - 1), min_length: 1, max_length: 1)
    ])
  end

  defp key_generator do
    first_chars = string_chars(?A..?Z) ++ string_chars(?a..?z) ++ ["_"]
    chars = first_chars ++ string_chars(?0..?9)

    StreamData.map(
      {StreamData.member_of(first_chars),
       StreamData.list_of(StreamData.member_of(chars), max_length: 7)},
      fn {first, rest} -> first <> Enum.join(rest) end
    )
  end

  defp string_chars(range), do: Enum.map(range, &<<&1::utf8>>)

  defp scalar_generator do
    StreamData.one_of([
      StreamData.string(:alphanumeric, min_length: 1, max_length: 12),
      StreamData.integer(),
      StreamData.float(min: -1_000.0, max: 1_000.0),
      StreamData.boolean()
    ])
  end

  defp scalar_list_generator do
    StreamData.one_of([
      StreamData.list_of(StreamData.string(:alphanumeric, min_length: 1, max_length: 12),
        min_length: 1,
        max_length: 4
      ),
      StreamData.list_of(StreamData.integer(), min_length: 1, max_length: 4),
      StreamData.list_of(StreamData.float(min: -1_000.0, max: 1_000.0),
        min_length: 1,
        max_length: 4
      ),
      StreamData.list_of(StreamData.boolean(), min_length: 1, max_length: 4)
    ])
  end

  defp expected_flat_map(map, prefix) do
    Enum.reduce(map, %{prefix => :map}, fn {key, value}, acc ->
      path = prefix <> "." <> key

      Map.merge(acc, expected_flat_entry(value, path))
    end)
  end

  defp expected_flat_entry(value, path) when is_map(value), do: expected_flat_map(value, path)

  defp expected_flat_entry(value, path) when is_list(value) do
    value
    |> Enum.filter(&is_map/1)
    |> expected_flat_entry_from_list(value, path)
  end

  defp expected_flat_entry(value, path), do: %{path => scalar_type(value)}

  defp expected_flat_entry_from_list([], value, path),
    do: %{path => {:list, scalar_type(hd(value))}}

  defp expected_flat_entry_from_list(maps, _value, path) do
    Enum.reduce(maps, %{path => :map}, fn item, acc ->
      Map.merge(acc, expected_flat_map(item, path))
    end)
  end

  defp scalar_type(value) when is_binary(value), do: :string
  defp scalar_type(value) when is_integer(value), do: :integer
  defp scalar_type(value) when is_float(value), do: :float
  defp scalar_type(value) when is_boolean(value), do: :boolean
end
