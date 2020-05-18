defmodule Logflare.Google.BigQuery.BqRepoTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.Google.BigQuery
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.BqRepo
  alias Logflare.Users
  import Logflare.Factory

  setup do
    u = Users.get_by(email: System.get_env("LOGFLARE_TEST_USER_WITH_SET_IAM"))
    s = insert(:source, user_id: u.id)
    {:ok, sources: [s], users: [u]}
  end

  describe "query" do
    test "returns nil rows for a new empty table", %{sources: [source], users: [u]} do
      %{
        bigquery_table_ttl: bigquery_table_ttl,
        bigquery_dataset_location: bigquery_dataset_location,
        bigquery_project_id: bigquery_project_id,
        bigquery_dataset_id: bigquery_dataset_id
      } = GenUtils.get_bq_user_info(source.token)

      {:ok, table} =
        BigQuery.init_table!(
          u.id,
          source.token,
          bigquery_project_id,
          bigquery_table_ttl,
          bigquery_dataset_location,
          bigquery_dataset_id
        )

      table_id = table.id |> String.replace(":", ".")
      sql = "SELECT timestamp FROM `#{table_id}`"

      {:ok, response} = BqRepo.query_with_sql_and_params(bigquery_project_id, sql, [])
      assert response.rows == []
      assert response.total_rows == "0"
    end
  end
end
