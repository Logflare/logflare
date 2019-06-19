defmodule Logflare.SourcesTest do
  @moduledoc false
  use Logflare.DataCase
  import Logflare.DummyFactory
  alias Logflare.Sources
  alias Logflare.Google.BigQuery
  alias Logflare.Google.BigQuery.GenUtils
  alias GoogleApi.BigQuery.V2.Model.TableSchema, as: TS
  alias GoogleApi.BigQuery.V2.Model.TableFieldSchema, as: TFS

  setup do
    u = insert(:user)
    s = insert(:source, token: Faker.UUID.v4(), rules: [], user_id: u.id)

    {:ok, sources: [s]}
  end

  describe "Sources" do
    @tag :skip
    test "get_bq_schema/1", %{sources: [s | _]} do
      source_id = s.token
      bigquery_project_id = GenUtils.get_project_id(source_id)
      bigquery_table_ttl = GenUtils.get_table_ttl(source_id)
      BigQuery.init_table!(source_id, bigquery_project_id, bigquery_table_ttl)

      schema = %TS{
        fields: [
          %TFS{
            description: nil,
            fields: nil,
            mode: "REQUIRED",
            name: "timestamp",
            type: "TIMESTAMP"
          },
          %TFS{
            description: nil,
            fields: nil,
            mode: "NULLABLE",
            name: "event_message",
            type: "STRING"
          },
          %TFS{
            description: nil,
            fields: [
              %TFS{
                description: nil,
                mode: "NULLABLE",
                name: "string1",
                type: "STRING",
                fields: nil
              }
            ],
            mode: "NULLABLE",
            name: "metadata",
            type: "RECORD"
          }
        ]
      }

      assert {:ok, _} = BigQuery.patch_table(source_id, schema, bigquery_project_id)

      assert Sources.get_bq_schema(s) == schema
    end
  end
end
