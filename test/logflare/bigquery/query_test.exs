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
    test "returns nil rows for a new empty table", %{sources: [source], users: [user]} do
      conn = GenUtils.get_conn()
      project_id = GenUtils.get_project_id(source.token)

      assert {:ok, _} = BigQuery.create_dataset("#{user.id}", project_id)
      assert {:ok, _} = BigQuery.create_table(source.token, project_id)

      table = source.token |> Atom.to_string() |> String.replace("-", "_")
      sql = "SELECT timestamp FROM `#{project_id}`.#{user.id}_test.`#{table}`"

      {:ok, response} = Query.query(conn, project_id, sql)
      assert is_nil(response.rows)
      assert response.totalRows == "0"
    end
  end
end
