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
      |> element("form")
      |> render_submit(%{
        live_monaco_editor: %{
          query: "select current_timestamp() as ts"
        }
      }) =~ "Ran query successfully"

      assert_patch(view) =~ ~r/current_timestamp/
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
