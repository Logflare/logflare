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
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, opts ->
        assert [body: %_{useQueryCache: false}] = opts

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

      assert view |> render() =~ ~r/1 .+ processed/

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

  describe "team context switching" do
    test "switches team context when query references source from another team via URL", %{
      conn: conn
    } do
      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([%{}])}
      end)

      team_owner = insert(:user)
      team = insert(:team, user: team_owner)
      insert(:source, user: team_owner, name: "team_source")

      team_user = insert(:team_user, email: "member@example.com", team: team)

      conn = login_user(conn, team_owner, team_user)

      query = URI.encode("SELECT id, timestamp FROM `team_source`")
      {:ok, view, _html} = live(conn, "/query?q=#{query}")

      html =
        view
        |> element("form")
        |> render_submit(%{})

      assert html =~ "Ran query successfully",
             "Expected successful query execution for team member accessing team source via URL. Got: #{String.slice(html, 0, 2000)}"
    end

    test "switches team context when query is submitted via form", %{conn: conn} do
      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([%{}])}
      end)

      team_owner = insert(:user)
      team = insert(:team, user: team_owner)
      insert(:source, user: team_owner, name: "form_source")

      team_user = insert(:team_user, email: "formmember@example.com", team: team)

      conn = login_user(conn, team_owner, team_user)

      {:ok, view, _html} = live(conn, "/query")

      view
      |> render_hook("parse-query", %{
        value: "SELECT id, timestamp FROM `form_source`"
      })

      html =
        view
        |> element("form")
        |> render_submit(%{})

      assert html =~ "Ran query successfully",
             "Expected successful query execution for team member submitting query via form. Got: #{String.slice(html, 0, 2000)}"
    end

    test "preserves team context when `t` param is provided", %{conn: conn, user: user} do
      team = insert(:team, user: user)
      insert(:source, user: user, name: "my_source")

      query = URI.encode("SELECT id, timestamp FROM `my_source`")

      {:ok, _view, html} = live(conn, "/query?q=#{query}&t=#{team.id}")

      assert html =~ "my_source"
    end

    test "shows error when running query with non-existent source", %{conn: conn} do
      {:ok, view, _html} = live(conn, "/query")

      view
      |> render_hook("parse-query", %{
        value: "SELECT id, timestamp, metadata FROM `nonexistent_source`"
      })

      html =
        view
        |> element("form")
        |> render_submit(%{})

      assert html =~ "can&#39;t find source nonexistent_source",
             "Expected error 'can't find source nonexistent_source' after running query. Got: #{String.slice(html, 0, 2000)}"
    end
  end

  describe "postgres backend" do
    TestUtils.setup_single_tenant(backend_type: :postgres)

    test "run a query against postgres", %{conn: conn} do
      {:ok, view, _html} = live(conn, "/query")

      render_hook(view, "parse-query", %{
        value: "select 42 as answer, 'and some other expected string'"
      })

      html =
        view
        |> element("form")
        |> render_submit(%{})

      assert html =~ "Ran query successfully"
      assert html =~ "0 bytes processed"
      assert html =~ "answer"
      assert html =~ "42"
      assert html =~ "and some other expected string"
    end
  end
end
