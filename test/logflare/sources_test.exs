defmodule Logflare.SourcesTest do
  @moduledoc false
  use Logflare.DataCase
  import Logflare.Factory
  use Logflare.Commons
  alias Ecto.Adapters.SQL.Sandbox
  alias Logflare.Google.BigQuery
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.Source.RecentLogsServer, as: RLS

  @moduletag :unboxed

  alias Logflare.Source
  @moduletag :this


  setup do
    u = user_with_iam()

    {:ok, s} = Sources.create_source(params_for(:source, token: Faker.UUID.v4(), rules: []), u)

    {:ok, pid} =
      Source.BigQuery.Schema.start_link(%RLS{
        source_id: s.token,
        plan: %{limit_source_fields_limit: 500}
      })

    :ok = Sandbox.allow(Repo, self(), pid)

    {:ok, sources: [s], users: [u]}
  end

  describe "Sources" do
    test "insert and get" do
      {:ok, u1} = Users.insert_or_update_user(params_for(:user))
      {:ok, s01} = Sources.create_source(params_for(:source), u1)
      {:ok, s02} = Sources.create_source(params_for(:source), u1)
      r1 = string_params_for(:rule, sink: s01.token, lql_string: "error")
      r2 = string_params_for(:rule, sink: s02.token, lql_string: "info")

      {:ok, source} = Sources.create_source(params_for(:source, token: Faker.UUID.v4()), u1)
      source = Sources.get_by_id_and_preload(source.id)
      {:ok, _} = Rules.create_rule(r1, source)
      {:ok, _} = Rules.create_rule(r2, source)

      {:ok, _s2} = Sources.create_source(params_for(:source, token: Faker.UUID.v4()), u1)

      left_source =
        Sources.get_source_by(token: source.token)
        |> Sources.preload_defaults()

      assert left_source.id == source.id
      assert left_source.inserted_at == source.inserted_at
      assert is_list(left_source.rules)
      assert length(left_source.rules) == 2
    end

    test "update bq schema", %{sources: [s | _], users: [u | _]} do
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

      assert :ok = Logflare.Source.BigQuery.Schema.update(source_id, schema)

      left_schema =
        Sources.get_source(source_id)
        |> Sources.preload_defaults()
        |> Map.get(:source_schema)
        |> Map.get(:bigquery_schema)

      assert left_schema == schema
    end
  end
end
