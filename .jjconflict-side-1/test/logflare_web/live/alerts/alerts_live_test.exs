defmodule LogflareWeb.AlertsLiveTest do
  use LogflareWeb.ConnCase

  alias Logflare.Backends.Adaptor.WebhookAdaptor
  alias Logflare.Backends.Adaptor.SlackAdaptor

  @update_attrs %{
    name: "some updated name",
    description: "some updated description",
    cron: "2 * * * * *",
    query: "select another from `my-source`",
    slack_hook_url: "some updated slack_hook_url",
    webhook_notification_url: "some updated webhook_notification_url"
  }

  setup %{conn: conn} do
    insert(:plan, name: "Free")
    user = insert(:user)
    team = insert(:team, user: user)
    conn = login_user(conn, user)
    start_supervised!(Logflare.Alerting.Supervisor)
    [user: user, team: team, conn: conn]
  end

  defp create_alert_query(%{user: user}) do
    %{alert_query: insert(:alert, user_id: user.id)}
  end

  describe "unauthorized" do
    test "redirects when accessing alert that doesn't belong to user", %{conn: conn} do
      user = insert(:user)
      other_user = insert(:user)
      alert = insert(:alert, user: other_user)

      conn =
        conn
        |> login_user(user)
        |> get(~p"/alerts/#{alert.id}")

      assert redirected_to(conn, 302) =~ "/alerts"
    end
  end

  describe "Index" do
    setup [:create_alert_query]

    setup do
      Logflare.Alerting.AlertsScheduler
      |> stub(:add_job, fn _ -> :ok end)

      :ok
    end

    test "mounts successfully with user_id from session", %{conn: conn, user: user} do
      # This reproduces a bug where list_sources_by_user/1 expects an integer
      conn = Plug.Test.init_test_session(conn, %{"user_id" => Integer.to_string(user.id)})

      assert {:ok, _view, html} = live(conn, Routes.alerts_path(conn, :index))
      assert html =~ "Alerts"
    end

    test "lists all alert_queries", %{conn: conn, alert_query: alert_query, team: team} do
      {:ok, view, html} = live(conn, Routes.alerts_path(conn, :index))
      assert html =~ alert_query.name
      assert html =~ "Alerts"
      # link to show
      view
      |> element("ul li a", alert_query.name)
      |> render_click()

      assert_patched(view, "/alerts/#{alert_query.id}?t=#{team.id}")
      assert has_element?(view, "h1,h2,h3,h4,h5", alert_query.name)
      assert has_element?(view, "p", alert_query.description)
      assert has_element?(view, "code", alert_query.query)
    end

    test "can attach new backend to the alert query", %{
      conn: conn,
      user: user,
      team: team,
      alert_query: alert_query
    } do
      backend = insert(:backend, user: user)

      {:ok, view, _html} = live(conn, ~p"/alerts/#{alert_query.id}")

      # toggle open the backend form
      view
      |> element("button", "Add Backend")
      |> render_click()

      assert view
             |> element("form#backend")
             |> render_submit(%{
               backend: %{backend_id: backend.id}
             }) =~ "Backend added successfully"

      html = render(view)
      assert html =~ backend.name

      # nav to show backend page
      view
      |> element("a", backend.name)
      |> render_click()

      assert_patched(view, ~p"/backends/#{backend.id}?t=#{team.id}")
      assert render(view) =~ alert_query.name
    end

    test "validates query", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/alerts/new")

      valid_query = "select current_timestamp() as my_time"
      invalid_query = "bad_query"

      # triggering event handler directly since Monaco does this via JavaScript
      assert view
             |> with_target("#alert_query_editor")
             |> render_hook("parse-query", %{"value" => invalid_query}) =~ "SQL Parse error!"

      refute view
             |> with_target("#alert_query_editor")
             |> render_hook("parse-query", %{"value" => valid_query}) =~ "SQL Parse error!"
    end

    test "show for nonexistent query", %{conn: conn} do
      {:error, {:live_redirect, %{flash: %{"info" => "Alert not found" <> _}}}} =
        live(conn, ~p"/alerts/123")
    end

    test "can remove backend from the alert query", %{
      conn: conn,
      user: user,
      alert_query: alert_query
    } do
      backend = insert(:backend, user: user, alert_queries: [alert_query])
      {:ok, view, html} = live(conn, ~p"/alerts/#{alert_query.id}")
      assert html =~ backend.name

      view
      |> element("button", "Remove backend")
      |> render_click() =~ "Backend removed successfully"

      refute has_element?(view, backend.name)
    end

    test "saves new alert_query", %{conn: conn, user: user} do
      {:ok, view, _html} = live(conn, Routes.alerts_path(conn, :index))
      user = Logflare.Repo.preload(user, :team)

      assert view
             |> element("a", "New alert")
             |> render_click() =~ ~r/\~\/.+alerts.+\/new/

      assert_patch(view, "/alerts/new?t=#{user.team.id}")

      new_query = "select current_timestamp() as my_time"

      assert view
             |> element("form#alert")
             |> render_submit(%{
               alert: %{
                 description: "some description",
                 name: "some alert query",
                 query: new_query,
                 cron: "0 0 * * * *",
                 language: "bq_sql"
               }
             }) =~ "Successfully created alert"

      # redirected to :show
      assert assert_patch(view) =~ ~r/\/alerts\/\S+/
      html = render(view)
      assert html =~ "some description"
      assert html =~ "some alert query"
      assert html =~ new_query
    end

    test "saves new alert_query with errors, shows flash message", %{conn: conn, user: user} do
      {:ok, view, _html} = live(conn, Routes.alerts_path(conn, :index))
      user = Logflare.Repo.preload(user, :team)

      assert view
             |> element("a", "New alert")
             |> render_click() =~ ~r/\~\/.+alerts.+\/new/

      assert_patch(view, "/alerts/new?t=#{user.team.id}")

      assert view
             |> element("form#alert")
             |> render_submit(%{
               alert: %{
                 description: "some description",
                 name: "some alert query",
                 query: "select current_timestamp() from `error query",
                 cron: "0 0 * * * *",
                 language: "bq_sql"
               }
             }) =~ "Could not create alert"

      assert view |> has_element?("form#alert")
    end

    test "update alert_query", %{conn: conn, alert_query: alert_query} do
      {:ok, view, _html} = live(conn, Routes.alerts_path(conn, :index))

      view
      |> element("li a", alert_query.name)
      |> render_click()

      view
      |> element("a", "edit")
      |> render_click()

      view
      |> element("form#alert")
      |> render_submit(%{
        alert: @update_attrs
      }) =~ "updated successfully"

      html = render(view)
      assert html =~ @update_attrs.name
      assert html =~ @update_attrs.query
      refute html =~ alert_query.name
      refute html =~ alert_query.query
    end

    test "deletes alert_query in listing", %{conn: conn, alert_query: alert_query} do
      {:ok, view, _html} = live(conn, ~p"/alerts/#{alert_query.id}/edit")

      assert view
             |> element("button", "Delete")
             |> render_click() =~ "has been deleted"

      assert_patch(view, "/alerts")
      refute view |> has_element?("li", alert_query.name)
    end

    test "remove slack hook", %{conn: conn, alert_query: alert_query} do
      {:ok, view, _html} = live(conn, ~p"/alerts/#{alert_query.id}")

      assert view
             |> element("button", "Remove Slack")
             |> render_click() =~ "Slack notifications have been removed"

      assert view
             |> element("img[alt='Add to Slack']")
             |> has_element?()
    end
  end

  describe "running alerts" do
    setup [:create_alert_query]

    setup do
      WebhookAdaptor.Client
      |> stub(:send, fn _ -> {:ok, %Tesla.Env{}} end)

      SlackAdaptor.Client
      |> stub(:send, fn _, _ -> {:ok, %Tesla.Env{}} end)

      :ok
    end

    test "manual alert trigger - notification sent", %{conn: conn, alert_query: alert_query} do
      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([%{"testing" => "results-123"}])}
      end)

      {:ok, view, _html} = live(conn, Routes.alerts_path(conn, :show, alert_query))

      assert view
             |> element("button", "Manual trigger")
             |> render_click() =~ "Alert has been triggered. Notifications sent!"
    end

    test "manual alert trigger - notification not sent", %{conn: conn, alert_query: alert_query} do
      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([])}
      end)

      {:ok, view, _html} = live(conn, Routes.alerts_path(conn, :show, alert_query))

      assert view
             |> element("button", "Manual trigger")
             |> render_click() =~
               "Alert has been triggered. No results from query, notifications not sent!"
    end

    test "errors from BQ are dispalyed", %{conn: conn, alert_query: alert_query} do
      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:error, TestUtils.gen_bq_error("some error")}
      end)

      {:ok, view, _html} = live(conn, Routes.alerts_path(conn, :show, alert_query))

      assert view
             |> element("button", "Manual trigger")
             |> render_click() =~ "some error"
    end
  end

  describe "run query" do
    setup [:create_alert_query]

    test "rows are displayed", %{conn: conn, alert_query: alert_query} do
      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, opts ->
        assert [body: %_{useQueryCache: false}] = opts
        {:ok, TestUtils.gen_bq_response([%{"testing" => "results-123"}])}
      end)

      {:ok, view, _html} = live(conn, Routes.alerts_path(conn, :show, alert_query))

      assert view
             |> element("button", "Run query")
             |> render_click() =~ "results-123"

      assert view |> render() =~ ~r/1 .+ processed/
    end

    test "errors from BQ are dispalyed", %{conn: conn, alert_query: alert_query} do
      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:error, TestUtils.gen_bq_error("some error")}
      end)

      {:ok, view, _html} = live(conn, Routes.alerts_path(conn, :show, alert_query))

      assert view
             |> element("button", "Run query")
             |> render_click() =~ "some error"
    end

    test "No rows returned", %{conn: conn, alert_query: alert_query} do
      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([])}
      end)

      {:ok, view, _html} = live(conn, Routes.alerts_path(conn, :show, alert_query))

      assert view
             |> element("button", "Run query")
             |> render_click() =~ "No rows returned"
    end
  end

  describe "resolving team context" do
    setup %{user: user, team: team} do
      team_user = insert(:team_user, team: team)
      alert_query = insert(:alert, user_id: user.id)

      [team_user: team_user, alert_query: alert_query]
    end

    test "team user can list alerts", %{
      conn: conn,
      user: user,
      team_user: team_user,
      alert_query: alert_query
    } do
      {:ok, _view, html} =
        conn
        |> login_user(user, team_user)
        |> live(~p"/alerts?t=#{team_user.team_id}")

      assert html =~ alert_query.name
    end

    test "team user can view alert without t= param", %{
      conn: conn,
      user: user,
      team_user: team_user,
      alert_query: alert_query
    } do
      {:ok, _view, html} =
        conn
        |> login_user(user, team_user)
        |> live(~p"/alerts/#{alert_query.id}")

      assert html =~ alert_query.name
    end

    test "alerts links preserve team param", %{
      conn: conn,
      user: user,
      team_user: team_user,
      alert_query: alert_query
    } do
      {:ok, _view, html} =
        conn |> login_user(user, team_user) |> live(~p"/alerts?t=#{team_user.team_id}")

      assert html =~ ~r/alerts\/#{alert_query.id}[^"<]*t=#{team_user.team_id}/
    end

    test "alert show links preserve team param", %{
      conn: conn,
      user: user,
      team_user: team_user,
      alert_query: alert_query
    } do
      {:ok, _view, html} =
        conn
        |> login_user(user, team_user)
        |> live(~p"/alerts/#{alert_query}?t=#{team_user.team_id}")

      for path <- ["alerts/#{alert_query.id}/edit", "access-tokens"] do
        assert html =~ ~r/#{path}[^"<]*t=#{team_user.team_id}/
      end
    end
  end
end
