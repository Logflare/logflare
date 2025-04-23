defmodule LogflareWeb.QueryLiveTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  setup %{conn: conn} do
    insert(:plan)
    user = insert(:user)
    conn = login_user(conn, user)
    {:ok, user: user, conn: conn}
  end

  describe "query page" do
    test "run a valid query", %{conn: conn} do
      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([%{"ts" => "some-data"}])}
      end)

      {:ok, view, _html} = live(conn, "/query")

      # link to show
      view
      |> render_hook("parse-query", %{
        value: "select current_timestamp() as ts"
      })

      view
      |> element("form")
      |> render_submit(%{}) =~ "Ran query successfully"

      assert_patch(view) =~ ~r/current_timestamp/
      assert render(view) =~ "some-data"
    end

    test "run a very long query", %{conn: conn} do
      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([%{"ts" => "some-data"}])}
      end)

      {:ok, view, _html} = live(conn, "/query")

      query = """
      with my_query as (
      SELECT current_timestamp() as ts
      ),
      other_query as (
      SELECT current_timestamp() as ts
      ),
      another as (
      SELECT current_timestamp() as ts
      ),
      my_query_other as (
      SELECT current_timestamp() as ts
      ),
      my_query_other1 as (
      SELECT current_timestamp() as ts
      ),
      my_query_other2 as (
      SELECT current_timestamp() as ts
      ),
      my_query_other3 as (
      SELECT current_timestamp() as ts
      ),
      my_query_other4 as (
      SELECT current_timestamp() as ts
      ),
      my_query_other5 as (
      SELECT current_timestamp() as ts
      ),
      my_test_query as (
      SELECT current_timestamp() as ts
      )
      select ts as ts1, ts as ts2 from my_query

      """

      view
      |> render_hook("parse-query", %{
        value: query
      }) =~ "my_test_query"

      assert view
             |> element("form")
             |> render_submit(%{}) =~ "Ran query successfully"

      assert_patch(view) =~ ~r/my_test_query/
      assert render(view) =~ "some-data"
    end

    test "parser error", %{conn: conn} do
      {:ok, view, _html} = live(conn, "/query")

      assert view
             |> render_hook("parse-query", %{
               value: "select current_datetime() order-by invalid"
             }) =~ "parser error"
    end
  end
end
