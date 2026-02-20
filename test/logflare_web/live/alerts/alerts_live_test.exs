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

    test "run query - results returned", %{conn: conn, alert_query: alert_query} do
      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([%{"testing" => "results-123"}])}
      end)

      {:ok, view, _html} = live(conn, Routes.alerts_path(conn, :show, alert_query))

      assert view
             |> element("button", "Run query")
             |> render_click() =~ "Query executed successfully. Alert will fire."
    end

    test "run query - no results", %{conn: conn, alert_query: alert_query} do
      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([])}
      end)

      {:ok, view, _html} = live(conn, Routes.alerts_path(conn, :show, alert_query))

      html =
        view
        |> element("button", "Run query")
        |> render_click()

      assert html =~ "No results from query. Alert will not fire."
    end

    test "errors from BQ are displayed", %{conn: conn, alert_query: alert_query} do
      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:error, TestUtils.gen_bq_error("some error")}
      end)

      {:ok, view, _html} = live(conn, Routes.alerts_path(conn, :show, alert_query))

      assert view
             |> element("button", "Run query")
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

    test "test query from edit page uses the submitted query", %{
      conn: conn,
      alert_query: alert_query
    } do
      test_query = "select current_timestamp() as test_col"

      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, opts ->
        assert [body: %_{query: query}] = opts
        assert query =~ "current_timestamp()"
        {:ok, TestUtils.gen_bq_response([%{"test_col" => "edit-results"}])}
      end)

      {:ok, view, _html} = live(conn, ~p"/alerts/#{alert_query.id}/edit")

      html =
        view
        |> element("form[phx-submit='run-query']")
        |> render_submit(%{query: test_query})

      assert html =~ "Query executed successfully"
      assert html =~ "edit-results"
    end

    test "No rows returned", %{conn: conn, alert_query: alert_query} do
      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([])}
      end)

      {:ok, view, _html} = live(conn, Routes.alerts_path(conn, :show, alert_query))

      assert view
             |> element("button", "Run query")
             |> render_click() =~ "No results from query"
    end
  end

  describe "execution history" do
    setup [:create_alert_query]

    defp insert_oban_job(alert_query, attrs) do
      now = DateTime.utc_now()

      defaults = %{
        state: "completed",
        queue: "alerts",
        worker: "Logflare.Alerting.AlertWorker",
        args: %{"alert_query_id" => alert_query.id, "scheduled_at" => DateTime.to_iso8601(now)},
        meta: %{},
        attempted_at: now,
        completed_at: now,
        scheduled_at: now,
        attempted_by: ["test_node"],
        max_attempts: 1
      }

      merged = Map.merge(defaults, attrs)

      %Oban.Job{}
      |> Ecto.Changeset.change(merged)
      |> Logflare.Repo.insert!()
    end

    test "displays past executions with results", %{conn: conn, alert_query: alert_query} do
      _job =
        insert_oban_job(alert_query, %{
          meta: %{
            "result" => %{
              "fired" => true,
              "rows" => [%{"col_a" => "val1", "col_b" => "val2"}],
              "total_bytes_processed" => 1_073_741_824,
              "total_rows" => 1
            }
          }
        })

      {:ok, view, html} = live(conn, ~p"/alerts/#{alert_query.id}")

      assert html =~ "Past Executions"
      assert html =~ "Fired"
      assert html =~ "1"
      assert html =~ "1.00 GB"

      # Click View Results to open modal
      view
      |> element("button", "View Results")
      |> render_click()

      html = render(view)
      assert html =~ "Execution Results"
      assert html =~ "col_a"
      assert html =~ "col_b"
      assert html =~ "val1"
      assert html =~ "val2"
      # show oban job attempted by node name
      assert html =~ "test_node"
    end

    test "displays not fired status", %{conn: conn, alert_query: alert_query} do
      insert_oban_job(alert_query, %{
        meta: %{"result" => %{"fired" => false, "rows" => []}}
      })

      {:ok, _view, html} = live(conn, ~p"/alerts/#{alert_query.id}")

      assert html =~ "Not Fired"
    end

    test "hides scheduled jobs toggle when no future jobs exist", %{
      conn: conn,
      alert_query: alert_query
    } do
      # Only insert a past job, no future jobs
      insert_oban_job(alert_query, %{
        state: "completed",
        meta: %{"result" => %{"fired" => false, "rows" => []}}
      })

      {:ok, _view, html} = live(conn, ~p"/alerts/#{alert_query.id}")

      assert html =~ "Past Executions"
      refute html =~ "Show scheduled jobs"
    end

    test "displays scheduled (future) jobs", %{conn: conn, alert_query: alert_query} do
      future_time = DateTime.add(DateTime.utc_now(), 3600, :second)

      insert_oban_job(alert_query, %{
        state: "scheduled",
        scheduled_at: future_time,
        completed_at: nil,
        attempted_at: nil,
        attempted_by: nil
      })

      {:ok, view, html} = live(conn, ~p"/alerts/#{alert_query.id}")

      assert html =~ "Execution History"

      # Toggle to show scheduled jobs
      view
      |> element("button", "Show scheduled jobs")
      |> render_click()

      html = render(view)
      assert html =~ "Upcoming Jobs"
      assert html =~ "scheduled"
    end

    test "displays error reason for failed jobs", %{conn: conn, alert_query: alert_query} do
      insert_oban_job(alert_query, %{
        state: "discarded",
        meta: %{"reason" => "BigQuery error: table not found"}
      })

      {:ok, _view, html} = live(conn, ~p"/alerts/#{alert_query.id}")

      assert html =~ "BigQuery error: table not found"
    end

    test "truncates long error reason with popover", %{conn: conn, alert_query: alert_query} do
      long_reason = String.duplicate("a", 120)

      insert_oban_job(alert_query, %{
        state: "discarded",
        meta: %{"reason" => long_reason}
      })

      {:ok, _view, html} = live(conn, ~p"/alerts/#{alert_query.id}")

      # Should show truncated text
      assert html =~ String.slice(long_reason, 0, 80) <> "..."
      # Full reason should be in data-content for popover
      assert html =~ "data-content=\"#{long_reason}\""
    end

    test "pagination works for past executions", %{
      conn: conn,
      alert_query: alert_query,
      team: team
    } do
      # Insert 25 completed jobs (page size is 20, so 2 pages)
      for i <- 1..25 do
        scheduled_at = DateTime.add(DateTime.utc_now(), -i * 60, :second)

        insert_oban_job(alert_query, %{
          args: %{
            "alert_query_id" => alert_query.id,
            "scheduled_at" => DateTime.to_iso8601(scheduled_at)
          },
          scheduled_at: scheduled_at,
          completed_at: DateTime.add(scheduled_at, 5, :second),
          attempted_at: scheduled_at,
          meta: %{"result" => %{"fired" => false, "rows" => [], "total_rows" => 0}}
        })
      end

      {:ok, view, html} = live(conn, ~p"/alerts/#{alert_query.id}")

      # Should show pagination and 25 total entries
      assert html =~ "Past Executions (25)"
      # Should have pagination links
      assert html =~ "page-item"

      view
      |> element("a.page-link", "2")
      |> render_click()

      # Navigate to page 2 via live_patch
      assert_patch(view, ~p"/alerts/#{alert_query.id}?t=#{team.id}&page=2")
    end
  end

  describe "enabled field" do
    setup [:create_alert_query]

    test "edit form shows enabled checkbox", %{conn: conn, alert_query: alert_query} do
      {:ok, _view, html} = live(conn, ~p"/alerts/#{alert_query.id}/edit")
      assert html =~ "enabled-checkbox"
      assert html =~ "Enabled"
    end

    test "show page displays Enabled badge", %{conn: conn, alert_query: alert_query} do
      {:ok, _view, html} = live(conn, ~p"/alerts/#{alert_query.id}")
      assert html =~ "Enabled"
    end

    test "index shows Disabled badge for disabled alert", %{conn: conn, user: user} do
      _disabled_alert = insert(:alert, user_id: user.id, enabled: false, name: "Disabled Alert")
      {:ok, _view, html} = live(conn, ~p"/alerts")
      assert html =~ "Disabled Alert"
      assert html =~ "Disabled"
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

      assert html =~ ~r/alerts\/#{alert_query.id}\/edit[^"<]*t=#{team_user.team_id}/
    end
  end
end
