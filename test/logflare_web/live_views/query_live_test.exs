defmodule LogflareWeb.QueryLiveTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor

  setup %{conn: conn} do
    insert(:plan)
    user = insert(:user)
    team = insert(:team, user: user)
    conn = login_user(conn, user)
    {:ok, user: user, team: team, conn: conn}
  end

  describe "query page" do
    test "run a valid query", %{conn: conn} do
      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, opts ->
        assert [body: %_{useQueryCache: false}] = opts

        {:ok, TestUtils.gen_bq_response([%{"ts" => "some-data"}])}
      end)

      {:ok, view, _html} = live_with_redirect(conn, ~p"/query")

      # link to show
      view
      |> render_hook("parse-query", %{
        value: "select current_timestamp() as ts"
      })

      assert submit_query_form(view, conn) =~ "Ran query successfully"

      assert view |> render() =~ ~r/1 .+ processed/

      assert_patch(view) =~ ~r/current_timestamp/
      assert render(view) =~ "some-data"
    end

    test "run a very long query", %{conn: conn} do
      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([%{"ts" => "some-data"}])}
      end)

      {:ok, view, _html} = live_with_redirect(conn, ~p"/query")

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

      assert submit_query_form(view, conn) =~ "Ran query successfully"

      assert_patch(view) =~ ~r/my_test_query/
      assert render(view) =~ "some-data"
    end

    test "backend errors display a generic message", %{conn: conn} do
      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:error, TestUtils.gen_bq_error("raw backend detail", reason: "backendError")}
      end)

      {:ok, view, _html} = live_with_redirect(conn, ~p"/query")

      view
      |> render_hook("parse-query", %{
        value: "select current_timestamp() as ts"
      })

      html = submit_query_form(view, conn)

      assert html =~ LogflareWeb.QueryErrorHelpers.generic_query_error_message()
      refute html =~ "raw backend detail"
    end

    test "parser error", %{conn: conn} do
      {:ok, view, _html} = live_with_redirect(conn, ~p"/query")

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

      query = "SELECT id, timestamp FROM `team_source`"
      {:ok, view, _html} = live(conn, ~p"/query?#{%{q: query, t: team.id}}")

      html = submit_query_form(view, conn)

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

      {:ok, view, _html} = live(conn, "/query?t=#{team.id}")

      view
      |> render_hook("parse-query", %{
        value: "SELECT id, timestamp FROM `form_source`"
      })

      html = submit_query_form(view, conn)

      assert html =~ "Ran query successfully",
             "Expected successful query execution for team member submitting query via form. Got: #{String.slice(html, 0, 2000)}"
    end

    test "switches team context when query references other team source", %{
      conn: conn,
      user: user
    } do
      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([%{}])}
      end)

      team_owner = insert(:user)
      other_team = insert(:team, user: team_owner)
      insert(:team_user, email: user.email, team: other_team)
      insert(:source, user: team_owner, name: "other_team_source")

      {:ok, view, _html} = live_with_redirect(conn, ~p"/query")

      view
      |> render_hook("parse-query", %{
        value: "SELECT id, timestamp FROM `other_team_source`"
      })

      html = submit_query_form(view, conn)

      assert html =~ "Ran query successfully"
      assert_patch(view) =~ ~s(t=#{other_team.id})
    end

    test "preserves team context when `t` param matches query source team", %{
      conn: conn,
      user: user
    } do
      user = Logflare.Repo.preload(user, :team)
      insert(:source, user: user, name: "my_source")

      query = URI.encode("SELECT id, timestamp FROM `my_source`")

      {:ok, _view, html} = live(conn, "/query?q=#{query}&t=#{user.team.id}")

      assert html =~ "my_source"
    end

    test "keeps requested team when t param is present and source name is ambiguous",
         %{
           conn: conn,
           user: user,
           team: team
         } do
      insert(:source, user: user, name: "canonical_source")

      team_owner = insert(:user)
      other_team = insert(:team, user: team_owner)
      insert(:team_user, email: user.email, team: other_team)
      insert(:source, user: team_owner, name: "canonical_source")

      query = "SELECT id, timestamp FROM `canonical_source`"

      assert {:ok, view, html} = live(conn, ~p"/query?#{%{q: query, t: team.id}}")

      assert html =~ ~s(/dashboard?t=#{team.id})
      assert render(view) =~ "canonical_source"
    end

    test "updates team param from uniquely matched source in query URL", %{
      conn: conn,
      user: user
    } do
      team_owner = insert(:user)
      other_team = insert(:team, user: team_owner)
      insert(:team_user, email: user.email, team: other_team)
      insert(:source, user: team_owner, name: "canonical_source")

      query = "SELECT id, timestamp FROM `canonical_source`"

      assert {:error, {:live_redirect, %{to: patch}}} =
               live(conn, ~p"/query?#{%{q: query}}")

      params = URI.parse(patch).query |> URI.decode_query()

      assert params["t"] == to_string(other_team.id)
      assert params["q"] == query
    end

    test "updates team param from source in query URL when t is already set", %{
      conn: conn,
      user: user,
      team: team
    } do
      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([%{}])}
      end)

      team_owner = insert(:user)
      other_team = insert(:team, user: team_owner)
      insert(:team_user, email: user.email, team: other_team)
      insert(:source, user: team_owner, name: "canonical_source")

      query = "SELECT id, timestamp FROM `canonical_source`"

      {:ok, view, _html} = live(conn, ~p"/query?#{%{q: query, t: team.id}}")

      view
      |> render_hook("parse-query", %{
        value: query
      })

      html = submit_query_form(view, conn)

      assert html =~ "Ran query successfully"
      assert_patch(view) =~ ~s(t=#{other_team.id})
    end

    test "shows error when running query with non-existent source", %{conn: conn} do
      {:ok, view, _html} = live_with_redirect(conn, ~p"/query")

      view
      |> render_hook("parse-query", %{
        value: "SELECT id, timestamp, metadata FROM `nonexistent_source`"
      })

      html = submit_query_form(view, conn)

      assert html =~ "can&#39;t find source nonexistent_source",
             "Expected error 'can't find source nonexistent_source' after running query. Got: #{String.slice(html, 0, 2000)}"
    end
  end
  describe "postgres backend" do
    TestUtils.setup_single_tenant(backend_type: :postgres)

    test "run a query against postgres", %{conn: conn} do
      {:ok, view, _html} = live_with_redirect(conn, ~p"/query")

      render_hook(view, "parse-query", %{
        value: "select 42 as answer, 'and some other expected string'"
      })

      html = submit_query_form(view, conn)

      assert html =~ "Ran query successfully"
      assert html =~ "answer"
      assert html =~ "42"
      assert html =~ "and some other expected string"

      refute html =~ "bytes processed"
    end
  end

  describe "backend selection" do
    test "displays only queryable backends in dropdown", %{conn: conn, user: user} do
      bq_backend = insert(:backend, user: user, type: :bigquery)
      ch_backend = insert(:backend, user: user, type: :clickhouse)
      webhook = insert(:backend, user: user, type: :webhook)

      {:ok, view, _html} = live(conn, "/query")

      html = render(view)

      assert html =~ bq_backend.name
      assert html =~ ch_backend.name

      refute html =~ webhook.name

      assert html =~ "Default (BigQuery)"
    end

    test "runs query against selected ClickHouse backend", %{conn: conn, user: user} do
      backend = insert(:backend, user: user, type: :clickhouse)

      expect(ClickHouseAdaptor, :execute_query, fn _backend, _query, _opts ->
        {:ok, [%{"ts" => "ch-data"}]}
      end)

      {:ok, view, _html} = live(conn, "/query")

      view
      |> element("form#query-form")
      |> render_change(%{backend: %{backend_id: backend.id}})

      view |> render_hook("parse-query", %{value: "SELECT now() as ts"})

      html =
        view
        |> element("form#query-form")
        |> render_submit(%{backend: %{backend_id: backend.id}})

      assert_patch(view) =~ "backend_id=#{backend.id}"

      assert html =~ "Ran query successfully"
      assert render(view) =~ "ch-data"
    end

    test "backend selectd by URL param", %{conn: conn, user: user} do
      backend = insert(:backend, user: user, type: :clickhouse)

      {:ok, view, _html} = live(conn, "/query?backend_id=#{backend.id}")
      html = render(view)

      assert html =~ ~s(selected="selected" value="#{backend.id}")
      assert html =~ backend.name
    end

    test "shows selected backend language in label when backend_id is set", %{
      conn: conn,
      user: user
    } do
      backend = insert(:backend, user: user, type: :clickhouse)

      {:ok, view, _html} = live(conn, "/query?backend_id=#{backend.id}")
      html = render(view)

      assert html =~ "Query Language: <span id=\"query-language\">ClickHouse SQL</span>"
    end

    test "defaults to BigQuery when no backend selected", %{conn: conn} do
      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([%{"ts" => "bq-data"}])}
      end)

      {:ok, view, _html} = live(conn, "/query")

      view |> render_hook("parse-query", %{value: "SELECT current_timestamp() as ts"})

      html = view |> element("form#query-form") |> render_submit(%{})

      assert html =~ "Ran query successfully"
      assert render(view) =~ "bq-data"
    end
  end

  defp submit_query_form(view, conn) do
    case view |> element("form") |> render_submit(%{}) do
      {:error, {:live_redirect, _}} = result ->
        {:ok, _view, html} = result |> follow_redirect(conn)
        html

      html when is_binary(html) ->
        html
    end
  end
end
