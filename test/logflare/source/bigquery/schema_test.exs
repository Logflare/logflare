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

  test "update/3 skips casts when schema process mailbox is full" do
    put_schema_config(max_message_queue_len: 1)

    pid = start_mailbox_holder!()
    send(pid, :queued)

    assert :ok = Schema.update(pid, %Logflare.LogEvent{id: "id", body: %{}}, build(:source))
    assert {:message_queue_len, 1} = Process.info(pid, :message_queue_len)

    send(pid, :stop)
  end

  test "update/3 casts when schema process mailbox is below limit" do
    put_schema_config(max_message_queue_len: 2)

    pid = start_mailbox_holder!()

    assert :ok = Schema.update(pid, %Logflare.LogEvent{id: "id", body: %{}}, build(:source))
    assert {:message_queue_len, 1} = Process.info(pid, :message_queue_len)

    send(pid, :stop)
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

    # subsequent updates do not increase mock count
    le = build(:log_event, source: source, metadata: %{"change" => 123})
    assert :ok = Schema.update(pid, le, source)
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

  defp start_mailbox_holder! do
    test_pid = self()

    pid =
      spawn_link(fn ->
        send(test_pid, {:ready, self()})

        receive do
          :stop -> :ok
        after
          5_000 -> :ok
        end
      end)

    assert_receive {:ready, ^pid}

    pid
  end

  defp put_schema_config(config) do
    old_config = Application.get_env(:logflare, Schema)

    on_exit(fn ->
      Application.put_env(:logflare, Schema, old_config)
    end)

    Application.put_env(:logflare, Schema, Keyword.merge(old_config, config))
  end
end
