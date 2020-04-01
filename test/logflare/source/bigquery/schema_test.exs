defmodule Logflare.Source.BigQuery.SchemaTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.Google.BigQuery, as: GoogleBigQuery
  alias Logflare.Source.BigQuery.Schema
  alias Logflare.Source.RecentLogsServer, as: RLS
  import Logflare.Factory
  use Placebo

  setup do
    u1 = insert(:user)
    s1 = insert(:source, user_id: u1.id)

    {:ok, sources: [s1]}
  end

  describe "Schema GenServer" do
    test "start_link/1", %{sources: [s1 | _]} do
      sid = s1.token
      rls = %RLS{source_id: sid}

      {:ok, _pid} = Schema.start_link(rls)

      schema = Schema.get_state(sid)

      assert %{schema | next_update: :placeholder} == %{
               bigquery_project_id: nil,
               bigquery_dataset_id: nil,
               field_count: nil,
               type_map: %{},
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
               next_update: :placeholder
             }
    end
  end
end
