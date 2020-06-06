defmodule Logflare.SourcesTest do
  @moduledoc false
  use Logflare.DataCase
  import Logflare.Factory
  alias Logflare.Sources
  alias Logflare.Users
  alias Logflare.Google.BigQuery
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.Source

  setup do
    u = Users.get_by(email: System.get_env("LOGFLARE_TEST_USER_WITH_SET_IAM"))
    s = insert(:source, token: Faker.UUID.v4(), rules: [], user_id: u.id)
    Source.BigQuery.Schema.start_link(%RLS{source_id: s.token})

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

      schema = %GoogleApi.BigQuery.V2.Model.TableSchema{
        fields: [
          %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
            categories: nil,
            description: nil,
            fields: nil,
            mode: "NULLABLE",
            name: "event_message",
            policyTags: nil,
            type: "STRING"
          },
          %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
            categories: nil,
            description: nil,
            fields: nil,
            mode: "NULLABLE",
            name: "id",
            policyTags: nil,
            type: "STRING"
          },
          %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
            categories: nil,
            description: nil,
            fields: nil,
            mode: "REQUIRED",
            name: "timestamp",
            policyTags: nil,
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
