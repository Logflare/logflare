defmodule Logflare.SourcesTest do
  @moduledoc false
  use Logflare.DataCase

  import Logflare.Factory

  alias Logflare.Google.BigQuery
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.Source
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.Sources
  alias Logflare.Sources.Counters
  alias Logflare.Users

  test "list_sources_by_user/1" do
    user = insert(:user)
    source = insert(:source, user: user)
    assert [%Source{}] = Sources.list_sources_by_user(user)
    assert [] == insert(:user) |> Sources.list_sources_by_user()
  end

  describe "Sources" do
    setup do
      u = Users.get_by(email: System.get_env("LOGFLARE_TEST_USER_WITH_SET_IAM"))
      s = insert(:source, token: Faker.UUID.v4(), rules: [], user_id: u.id)

      Source.BigQuery.Schema.start_link(%RLS{
        source_id: s.token,
        plan: %{limit_source_fields_limit: 500}
      })

      {:ok, sources: [s], users: [u]}
    end

    @tag :failing
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

  describe "preload_for_dashboard/1" do
    setup do
      Counters.start_link()

      %{user: insert(:user)}
    end

    test "preloads required fields", %{user: user} do
      sources = insert_list(3, :source, %{user: user})
      sources = Sources.preload_for_dashboard(sources)

      assert Enum.all?(sources, &Ecto.assoc_loaded?(&1.user))
      assert Enum.all?(sources, &Ecto.assoc_loaded?(&1.rules))
      assert Enum.all?(sources, &Ecto.assoc_loaded?(&1.saved_searches))
    end

    test "sorts data by name and favorite flag", %{user: user} do
      source_1 = insert(:source, %{user: user, name: "C"})
      source_2 = insert(:source, %{user: user, name: "B", favorite: true})
      source_3 = insert(:source, %{user: user, name: "A"})
      sources = Sources.preload_for_dashboard([source_1, source_2, source_3])

      assert Enum.map(sources, & &1.name) == Enum.map([source_2, source_3, source_1], & &1.name)
    end
  end
end
