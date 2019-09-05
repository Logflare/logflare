defmodule Logflare.Google.BigQuery.QueryTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.Google.BigQuery
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.Google.BigQuery.Query
  import Logflare.DummyFactory

  setup do
    u = insert(:user, email: System.get_env("LOGFLARE_TEST_USER_WITH_SET_IAM"))
    s = insert(:source, user_id: u.id)
    {:ok, sources: [s], users: [u]}
  end

  describe "query" do
    test "returns nil rows for a new empty table", %{sources: [source], users: [u]} do
      conn = GenUtils.get_conn()

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

      {:ok, response} = Query.query(conn, bigquery_project_id, sql)
      assert is_nil(response.rows)
      assert response.totalRows == "0"
    end
  end
end
