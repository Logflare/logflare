defmodule Logflare.Source.BigQuery.SchemaTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.Source.BigQuery.Schema
  alias Logflare.Source.RecentLogsServer, as: RLS
  import Logflare.Factory
  alias Logflare.Google.BigQuery.SchemaUtils

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

    # mock
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

    # mock goth behaviour
    Goth
    |> stub(:fetch, fn _mod -> {:ok, %Goth.Token{token: "auth-token"}} end)

    Logflare.Mailer
    |> expect(:deliver, 1, fn _ -> :ok end)

    Logflare.Sources
    |> expect(:get_by_and_preload, fn _ -> source end)
    |> expect(:get_by, fn _ -> source end)

    rls = %RLS{source_id: source.token, plan: %{limit_source_fields_limit: 500}}

    start_supervised!({Schema, rls})

    state = Schema.get_state(source.token)
    initial_ts = state.next_update

    assert String.length("#{state.next_update}") ==
             String.length("#{System.system_time(:millisecond)}")

    # Be sure that some time have passed
    :timer.sleep(10)

    # trigger an update
    le = build(:log_event, source: source, metadata: %{"test" => 123})
    assert :ok = Schema.update(source.token, le)
    %{next_update: updated_ts} = Schema.get_state(source.token)
    assert updated_ts != initial_ts
    # try to update again with different le
    le = build(:log_event, source: source, metadata: %{"change" => 123})
    assert :ok = Schema.update(source.token, le)
    %{next_update: unchanged_ts} = Schema.get_state(source.token)
    assert unchanged_ts == updated_ts
  end

  describe "Schema GenServer" do
    setup do
      u1 = insert(:user)
      s1 = insert(:source, user_id: u1.id)

      {:ok, sources: [s1]}
    end

    test "start_link/1", %{sources: [s1 | _]} do
      sid = s1.token
      rls = %RLS{source_id: sid, plan: %{limit_source_fields_limit: 500}}

      {:ok, _pid} = Schema.start_link(rls)

      schema = Schema.get_state(sid)

      assert %{schema | next_update: :placeholder} == %{
               bigquery_project_id: nil,
               bigquery_dataset_id: nil,
               schema: %GoogleApi.BigQuery.V2.Model.TableSchema{
                 fields: [
                   %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                     categories: nil,
                     description: nil,
                     fields: nil,
                     mode: "REQUIRED",
                     name: "timestamp",
                     type: "TIMESTAMP"
                   },
                   %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                     categories: nil,
                     description: nil,
                     fields: nil,
                     mode: "NULLABLE",
                     name: "id",
                     type: "STRING"
                   },
                   %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                     categories: nil,
                     description: nil,
                     fields: nil,
                     mode: "NULLABLE",
                     name: "event_message",
                     type: "STRING"
                   }
                 ]
               },
               source_token: sid,
               field_count: 3,
               type_map: %{
                 event_message: %{t: :string},
                 id: %{t: :string},
                 timestamp: %{t: :datetime}
               },
               next_update: :placeholder,
               field_count_limit: 500
             }
    end
  end
end
