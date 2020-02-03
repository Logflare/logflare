defmodule Logflare.SourcesTest do
  @moduledoc false
  use Logflare.DataCase
  import Logflare.Factory
  alias Logflare.Sources
  alias Logflare.Users
  alias Logflare.Google.BigQuery
  alias Logflare.Google.BigQuery.GenUtils
  alias GoogleApi.BigQuery.V2.Model.TableSchema, as: TS
  alias GoogleApi.BigQuery.V2.Model.TableFieldSchema, as: TFS

  setup do
    u = Users.get_by(email: System.get_env("LOGFLARE_TEST_USER_WITH_SET_IAM"))
    s = insert(:source, token: Faker.UUID.v4(), rules: [], user_id: u.id)

    {:ok, sources: [s], users: [u]}
  end

  describe "Sources" do
    test "get_bq_schema/1", %{sources: [s | _], users: [u | _]} do
      source_id = s.token

      %{
        bigquery_table_ttl: bigquery_table_ttl,
        bigquery_dataset_location: bigquery_dataset_location,
        bigquery_project_id: bigquery_project_id,
        bigquery_dataset_id: bigquery_dataset_id
      } = GenUtils.get_bq_user_info(source_id)

      BigQuery.init_table!(
        u.id,
        source_id,
        bigquery_project_id,
        bigquery_table_ttl,
        bigquery_dataset_location,
        bigquery_dataset_id
      )

      schema = %TS{
        fields: [
          %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
            categories: nil,
            description: nil,
            fields: nil,
            policyTags: nil,
            mode: "NULLABLE",
            name: "event_message",
            type: "STRING"
          },
          %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
            categories: nil,
            description: nil,
            fields: nil,
            mode: "NULLABLE",
            policyTags: nil,
            type: "STRING",
            name: "id"
          },
          %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
            categories: nil,
            description: nil,
            mode: "NULLABLE",
            policyTags: nil,
            fields: [
              %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                categories: nil,
                description: nil,
                fields: nil,
                mode: "NULLABLE",
                name: "string1",
                policyTags: nil,
                type: "STRING"
              }
            ],
            name: "metadata",
            type: "RECORD"
          },
          %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
            categories: nil,
            description: nil,
            policyTags: nil,
            fields: nil,
            mode: "REQUIRED",
            name: "timestamp",
            type: "TIMESTAMP"
          }
        ]
      }

      assert {:ok, _} =
               BigQuery.patch_table(source_id, schema, bigquery_dataset_id, bigquery_project_id)

      {:ok, left_schema} = Sources.get_bq_schema(s)
      assert left_schema == schema
    end
  end
end
