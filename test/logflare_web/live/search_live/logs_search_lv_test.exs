defmodule LogflareWeb.Source.SearchLVTest do
  @moduledoc false
  use LogflareWeb.ConnCase, async: false

  import Phoenix.LiveViewTest

  alias Logflare.SingleTenant
  alias Logflare.Sources.Source.BigQuery.Schema
  alias LogflareWeb.Source.SearchLV

  @endpoint LogflareWeb.Endpoint
  @default_search_params %{
    "querystring" => "c:count(*) c:group_by(t::minute)",
    "chart_period" => "minute",
    "chart_aggregate" => "count",
    "tailing?" => "false"
  }

  defp setup_mocks(_ctx) do
    stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, opts ->
      query = opts[:body].query

      response = %{
        "event_message" => Jason.encode!(%{"message" => "some event message"})
      }

      response =
        if query =~ "user_id" do
          Map.put(response, "user_id", "123")
        else
          response
        end

      {:ok, TestUtils.gen_bq_response(response)}
    end)

    :ok
  end

  defp on_exit_kill_tasks(_ctx) do
    on_exit(fn ->
      # Kill all tasks first
      Logflare.Utils.Tasks.kill_all_tasks()

      # Give processes time to clean up
      Process.sleep(10)

      :ok
    end)

    :ok
  end

  # to simulate signed in user.
  defp setup_user_session(%{conn: conn, user: user, plan: plan}) do
    _billing_account = insert(:billing_account, user: user, stripe_plan_id: plan.stripe_id)
    user = user |> Logflare.Repo.preload(:billing_account)
    conn = conn |> login_user(user)
    [conn: conn]
  end

  defp setup_team_user_session(%{conn: conn, user: user, plan: plan, team_user: team_user}) do
    _billing_account = insert(:billing_account, user: user, stripe_plan_id: plan.stripe_id)
    user = user |> Logflare.Repo.preload(:billing_account)

    [conn: login_user(conn, user, team_user)]
  end

  # do this for all tests
  setup [:setup_mocks, :on_exit_kill_tasks]
  setup {TestUtils, :attach_wait_for_render}

  describe "resource switching for team_users" do
    setup %{conn: conn} do
      plan = insert(:plan)
      [user1, user2] = insert_pair(:user)
      _billing_account = insert(:billing_account, user: user1, stripe_plan_id: plan.stripe_id)
      _billing_account = insert(:billing_account, user: user2, stripe_plan_id: plan.stripe_id)
      user1 = user1 |> Logflare.Repo.preload(:billing_account)
      user2 = user2 |> Logflare.Repo.preload(:billing_account)
      team1 = insert(:team, user: user1)
      team2 = insert(:team, user: user2)

      # person is invited to both teams
      email = "test@example.com"
      team_user1 = insert(:team_user, team: team1, email: email)
      team_user2 = insert(:team_user, team: team2, email: email)

      [
        conn: conn,
        user1: user1,
        user2: user2,
        plan: plan,
        team_user1: team_user1,
        team_user2: team_user2
      ]
    end

    test "invited team user can view source", %{
      conn: conn,
      user1: user1,
      team_user1: team_user1,
      team_user2: _team_user2
    } do
      source = insert(:source, user: user1)
      # set session for team_user1
      conn = conn |> login_user(team_user1.team.user, team_user1)
      {:ok, view, _html} = live(conn, ~p"/sources/#{source.id}/search")
      assert view |> element(".subhead") |> render() =~ source.name
    end

    test "switches team automatically if viewing", %{
      conn: conn,
      user1: user1,
      team_user1: team_user1,
      team_user2: team_user2
    } do
      source = insert(:source, user: user1)
      # set session for team_user1
      conn = conn |> login_user(team_user1.team.user, team_user2)
      {:ok, view, _html} = live(conn, ~p"/sources/#{source.id}/search")
      assert view |> element(".subhead") |> render() =~ source.name
    end

    test "uninvited user cannot view source", %{
      conn: conn,
      user1: user1,
      user2: user2,
      team_user1: _team_user1,
      team_user2: _team_user2
    } do
      # other_user = insert(:user)
      # other_team_user = insert(:team_user, team: team_user2.team, email: other_user.email)
      source = insert(:source, user: user1)
      # set session for team_user2
      conn = conn |> login_user(user2)
      assert conn |> get(~p"/sources/#{source.id}/search") |> html_response(404)
    end

    test "uninvited team user cannot view source", %{
      conn: conn,
      user1: user1,
      user2: _user2,
      team_user1: _team_user1,
      team_user2: _team_user2
    } do
      other_user = insert(:user)
      other_team = insert(:team, user: other_user)
      other_team_user = insert(:team_user, team: other_team)
      source = insert(:source, user: user1)
      # set session for team_user2
      conn = conn |> login_user(other_user, other_team_user)
      assert conn |> get(~p"/sources/#{source.id}/search") |> html_response(404)
    end
  end

  describe "no timezone preference for user" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)
      plan = insert(:plan)
      [user: user, source: source, plan: plan]
    end

    setup [:setup_user_session]

    test "subheader - default timezone is Etc/UTC", %{conn: conn, source: source} do
      {:ok, view, _html} = live(conn, ~p"/sources/#{source.id}/search")

      assert view
             |> element("#results-actions_search_timezone option[selected]")
             |> render() =~ "UTC"
    end

    test "subheader - switch dispalyed timezone with dropdown", %{conn: conn, source: source} do
      {:ok, view, _html} = live(conn, ~p"/sources/#{source.id}/search")

      assert view
             |> element(".subhead form#results-actions")
             |> render_change(%{search_timezone: "Asia/Singapore"})

      html = view |> element("#logs-list-container") |> render()

      assert html =~ "+08:00"
    end

    test "subheader - switch displayed timezone with UTC button", %{conn: conn, source: source} do
      {:ok, view, _html} = live(conn, ~p"/sources/#{source.id}/search?tz=Singapore")

      assert view |> element(".subhead") |> render() =~ "(+08:00)"

      assert view
             |> element(".subhead form#results-actions button[phx-value-search_timezone]")
             |> render_click()

      TestUtils.retry_assert(fn ->
        assert view |> element("#logs-list-container") |> render() =~ "+00:00"
      end)
    end

    test "subheader - checkbox is checked when loaded timzone matches user preference", %{
      conn: conn,
      user: user,
      source: source
    } do
      {:ok, _user} =
        Logflare.Users.update_user_with_preferences(user, %{
          preferences: %{timezone: "Asia/Singapore"}
        })

      {:ok, view, _html} =
        live(conn, ~p"/sources/#{source.id}/search?querystring=&tailing=F&tz=Asia/Singapore")

      assert view
             |> element(
               ".subhead form#results-actions #results-actions_remember_timezone:checked"
             )

      assert view
             |> element("#results-actions_search_timezone option[selected]")
             |> render() =~ "Asia/Singapore"
    end

    test "subheader - load with timezone in url even if it differs from preference", %{
      conn: conn,
      source: source
    } do
      {:ok, view, _html} =
        live(conn, ~p"/sources/#{source.id}/search?querystring=&tailing%3F=&tz=Singapore")

      assert view |> element(".subhead") |> render() =~ "(+08:00)"
    end
  end

  describe "preference timezone for user" do
    setup do
      user = insert(:user, preferences: build(:user_preferences, timezone: "US/Arizona"))
      source = insert(:source, user: user)
      plan = insert(:plan)
      [user: user, source: source, plan: plan]
    end

    setup [:setup_user_session]

    test "subheader - if no tz, will redirect to preference tz", %{conn: conn, source: source} do
      {:error, {:live_redirect, %{to: to}}} =
        live(conn, ~p"/sources/#{source.id}/search?querystring=something123&tailing%3F=")

      assert to =~ "US%2FArizona"
      assert to =~ "something123"
    end

    test "subheader - if ?tz=, will use param tz", %{
      conn: conn,
      source: source
    } do
      {:ok, view, _html} =
        live(
          conn,
          ~p"/sources/#{source.id}/search?querystring=something123&tailing%3F=&tz=Singapore"
        )

      assert view |> element(".subhead") |> render() =~ "(+08:00)"
      assert render(view) =~ "something123"
    end

    test "chart - timezone passed to chart component", %{conn: conn, source: source} do
      {:ok, view, _html} =
        live(
          conn,
          ~p"/sources/#{source.id}/search?querystring=something123&tailing%3F=&tz=Singapore"
        )

      assert view
             |> element(~s|div[data-live-react-class="Components.LogEventsChart"]|)
             |> render() =~
               Plug.HTML.html_escape(~s|"display_timezone":"Singapore"|)
    end
  end

  describe "preference timezone for team_user" do
    setup do
      %{user: user} = team = insert(:team)

      team_user =
        insert(:team_user, team: team, preferences: build(:user_preferences, timezone: "NZ"))

      source = insert(:source, user: user)
      plan = insert(:plan)
      [user: user, source: source, plan: plan, team_user: team_user]
    end

    setup [:setup_team_user_session]

    test "subheader - if no tz, will redirect to preference tz", %{
      conn: conn,
      source: source,
      team_user: team_user
    } do
      {:error, {:live_redirect, %{to: to}}} =
        live(
          conn,
          ~p"/sources/#{source.id}/search?t=#{team_user.team_id}&querystring=something123&tailing%3F="
        )

      assert to =~ "tz=NZ"
      assert to =~ "something123"
    end

    test "subheader - if ?tz=, will use param tz", %{
      conn: conn,
      source: source,
      team_user: team_user
    } do
      {:ok, view, _html} =
        live(
          conn,
          ~p"/sources/#{source.id}/search?t=#{team_user.team_id}&querystring=something123&tailing%3F=&tz=Singapore"
        )

      view
      |> TestUtils.wait_for_render("#logs-list-container li")

      assert view |> element(".subhead") |> render() =~ "(+08:00)"
      assert render(view) =~ "something123"
    end
  end

  describe "search tasks" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user, bigquery_clustering_fields: "user_id")
      plan = insert(:plan)

      bq_schema = TestUtils.build_bq_schema(%{"user_id" => "some_value"})

      insert(:source_schema,
        source: source,
        bigquery_schema: bq_schema,
        schema_flat_map: Logflare.Google.BigQuery.SchemaUtils.bq_schema_to_flat_typemap(bq_schema)
      )

      [user: user, source: source, plan: plan]
    end

    setup [:setup_mocks, :setup_user_session]

    test "subheader - lql docs", %{conn: conn, source: source} do
      {:ok, view, _html} = live(conn, ~p"/sources/#{source.id}/search?querystring=something123")

      assert view
             |> element("a", "LQL")
             |> render_click() =~ "Event Message Filtering"
    end

    test "subheader - schema modal", %{conn: conn, source: source} do
      {:ok, view, _html} = live(conn, ~p"/sources/#{source.id}/search")

      assert view
             |> element(".subhead a", "schema")
             |> render_click() =~ "event_message"
    end

    test "subheader - events", %{conn: conn, source: source} do
      {:ok, view, _html} = live(conn, ~p"/sources/#{source.id}/search")

      assert view
             |> element(".subhead a", "events")
             |> render_click()

      view
      |> TestUtils.wait_for_render(".search-query-debug")

      html = render(view)
      assert html =~ "Actual SQL query used when querying for results"

      formatted_sql =
        """
        SELECT
          t0.timestamp
        """
        |> String.trim()

      assert html =~ formatted_sql

      {:error, {:redirect, %{to: dest}}} =
        view
        |> element("a.btn.btn-primary", "Edit as query")
        |> render_click()

      assert dest =~ "/query?q=SELECT"
    end

    test "subheader - aggregeate", %{conn: conn, source: source} do
      {:ok, view, _html} = live(conn, ~p"/sources/#{source.id}/search")

      %{executor_pid: search_executor_pid} = get_view_assigns(view)
      Ecto.Adapters.SQL.Sandbox.allow(Logflare.Repo, self(), search_executor_pid)

      assert view
             |> element(".subhead a", "aggregate")
             |> render_click()

      view
      |> TestUtils.wait_for_render("#logflare-modal #search-query-debug p")

      html = render(view)

      assert html =~ "Actual SQL query used when querying for results"

      formatted_sql =
        """
        SELECT
          (
            CASE
        """
        |> String.trim()

      assert html =~ formatted_sql
    end

    test "subheader - saved searches", %{conn: conn, source: source} do
      {:ok, view, _html} = live(conn, ~p"/sources/#{source.id}/search")
      %{executor_pid: search_executor_pid} = get_view_assigns(view)
      Ecto.Adapters.SQL.Sandbox.allow(Logflare.Repo, self(), search_executor_pid)

      assert view
             |> element(".subhead a", "saved")
             |> render_click()

      view
      |> TestUtils.wait_for_render("#logflare-modal")

      assert view
             |> has_element?("#logflare-modal #saved-searches-empty")

      saved_search = insert(:saved_search, %{source: source})

      _ = Logflare.SavedSearches.Cache.bust_by(source_id: saved_search.source_id)
      {:ok, view, _html} = live(conn, ~p"/sources/#{source.id}/search")

      assert view
             |> element(".subhead a", "saved")
             |> render_click()

      view
      |> TestUtils.wait_for_render("#logflare-modal")

      view
      |> TestUtils.wait_for_render("#logflare-modal #saved-searches-list")

      assert view
             |> has_element?("#logflare-modal #saved-searches-list", saved_search.querystring)
    end

    test "load page", %{conn: conn, source: source} do
      {:ok, view, html} = live(conn, Routes.live_path(conn, SearchLV, source.id))
      %{executor_pid: search_executor_pid} = get_view_assigns(view)
      Ecto.Adapters.SQL.Sandbox.allow(Logflare.Repo, self(), search_executor_pid)

      assert html =~ "~/logs/"
      assert html =~ source.name
      assert html =~ "/search"

      view
      |> TestUtils.wait_for_render("#logs-list-container li")

      html = view |> element("#logs-list-container") |> render()
      assert html =~ "some event message"

      html = render(view)
      assert html =~ "Elapsed since last query"

      assert view
             |> has_element?("#logs-list-container a", "permalink")

      # permalink should have timestamp query parameter
      assert view
             |> element("#logs-list-container a", ~r/permalink/)

      assert view
             |> has_element?("#logs-list-container a[href*='timestamp']", "permalink")

      assert view
             |> has_element?("#logs-list-container a[href*='uuid']", "permalink")

      # includes recommended fields in permalink
      assert view |> element("#logs-list-container a[href]", "permalink") |> render =~
               URI.encode_query(%{"lql" => "user_id:123 c:count(*) c:group_by(t::minute)"})

      # default input values
      assert find_selected_chart_period(html) == "minute"
      assert find_chart_aggregate(html) == "count"

      querystring = find_querystring(html)

      assert querystring =~ "c:count(*) c:group_by(t::minute)"
    end

    test "empty results message", %{conn: conn, source: source} do
      pid = self()

      stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, opts ->
        query = opts[:body].query

        if query =~ ~r/COUNT\(|COUNTIF\(/i do
          send(pid, {:agg_query, query})
          {:ok, TestUtils.gen_bq_response([])}
        else
          send(pid, {:event_query, query})
          {:ok, TestUtils.gen_bq_response([])}
        end
      end)

      {:ok, view, _html} = live(conn, Routes.live_path(conn, SearchLV, source.id))
      %{executor_pid: search_executor_pid} = get_view_assigns(view)
      Ecto.Adapters.SQL.Sandbox.allow(Logflare.Repo, self(), search_executor_pid)

      view
      |> TestUtils.wait_for_render("#source-logs-search-list")

      assert_receive {:event_query, _query}
      assert_receive {:agg_query, _query}

      TestUtils.retry_assert(fn ->
        html = view |> element("#logs-list-container") |> render()

        assert html =~ "No events matching your query"
        refute html =~ "Extend search"
      end)
    end

    test "extend search button shows when aggregate results have hits", %{
      conn: conn,
      source: source
    } do
      pid = self()

      zero_dt = ~U[2026-01-30 06:46:41Z]
      hits_dt = ~U[2026-01-30 06:47:41Z]

      zero_ts_exp = TestUtils.gen_bq_timestamp(zero_dt)
      hits_ts_exp = TestUtils.gen_bq_timestamp(hits_dt)

      expected_zero_ts =
        zero_dt
        |> DateTime.truncate(:second)
        |> DateTime.to_iso8601()
        |> String.trim_trailing("Z")

      expected_hits_ts =
        hits_dt
        |> DateTime.truncate(:second)
        |> DateTime.to_iso8601()
        |> String.trim_trailing("Z")

      stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, opts ->
        query = opts[:body].query

        if query =~ ~r/COUNT\(|COUNTIF\(/i do
          send(pid, {:agg_query, query})
          aggregate_schema = Logflare.TestUtils.build_bq_schema(%{"value" => "INTEGER"})

          rows =
            [
              %{"timestamp" => zero_ts_exp, "value" => 0},
              %{"timestamp" => hits_ts_exp, "value" => 5}
            ]

          {:ok, TestUtils.gen_bq_response(rows, aggregate_schema)}
        else
          send(pid, {:event_query, query})
          {:ok, TestUtils.gen_bq_response([])}
        end
      end)

      {:ok, view, _html} = live(conn, Routes.live_path(conn, SearchLV, source.id))
      %{executor_pid: search_executor_pid} = get_view_assigns(view)
      Ecto.Adapters.SQL.Sandbox.allow(Logflare.Repo, self(), search_executor_pid)

      view
      |> TestUtils.wait_for_render("#source-logs-search-list")

      assert_receive {:event_query, _query}
      assert_receive {:agg_query, _query}

      html = view |> element("#logs-list-container") |> render()

      assert html =~ "No events matching your query"

      {:ok, document} = Floki.parse_document(html)

      assert [link] =
               document
               |> Floki.find("a")
               |> Enum.filter(fn link -> Floki.text(link) =~ "Extend search" end)

      assert Floki.text(document) =~ "t:>=#{expected_hits_ts}"
      refute Floki.text(document) =~ "t:>=#{expected_zero_ts}"

      href =
        link
        |> Floki.attribute("href")
        |> hd()

      uri = URI.parse(href)
      assert uri.path == "/sources/#{source.id}/search"

      query_params = URI.decode_query(uri.query)
      assert query_params["tailing?"] == "false"
      assert query_params["querystring"] =~ "t:>=#{expected_hits_ts}"
    end

    test "page title includes source name", %{conn: conn, source: source} do
      {:ok, _view, html} = live(conn, Routes.live_path(conn, SearchLV, source.id))
      assert html =~ "<title>#{source.name} | Logflare"
    end

    test "lql filters", %{conn: conn, source: source} do
      {:ok, view, _html} = live(conn, Routes.live_path(conn, SearchLV, source.id))
      pid = self()
      %{executor_pid: search_executor_pid} = get_view_assigns(view)
      Ecto.Adapters.SQL.Sandbox.allow(Logflare.Repo, self(), search_executor_pid)

      view
      |> TestUtils.wait_for_render("#logs-list-container li")

      html = view |> element("#logs-list-container") |> render()
      assert html =~ "some event message"

      stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, opts ->
        params = opts[:body].queryParameters

        if length(params) > 2 do
          assert Enum.any?(params, fn param -> param.parameterValue.value == "crasher" end)
          assert Enum.any?(params, fn param -> param.parameterValue.value == "error" end)
        end

        send(pid, {:query_request, opts[:body]})
        {:ok, TestUtils.gen_bq_response(%{"event_message" => "some error message"})}
      end)

      render_change(view, :form_update, %{
        "search" => %{
          @default_search_params
          | "querystring" => "c:count(*) c:group_by(t::minute) error crasher"
        }
      })

      view
      |> TestUtils.wait_for_render("#logs-list-container li")

      render_change(view, :start_search, %{
        "search" => %{
          "querystring" => "c:count(*) c:group_by(t::minute) error crasher"
        }
      })

      # wait for async search task to complete
      view
      |> TestUtils.wait_for_render("#logs-list-container li")

      html = view |> element("#logs-list-container") |> render()

      assert html =~ "some error message"
      refute html =~ "some event message"

      assert_receive {:query_request,
                      %_{jobCreationMode: "JOB_CREATION_OPTIONAL", parameterMode: "POSITIONAL"}}
    end

    test "count distinct aggregation", %{conn: conn, source: source} do
      pid = self()

      stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, opts ->
        if opts[:body].query =~ "COUNT(DISTINCT" do
          send(pid, {:ok, :countd})
        end

        {:ok, TestUtils.gen_bq_response(%{"event_message" => "test message"})}
      end)

      {:ok, view, _html} = live(conn, Routes.live_path(conn, SearchLV, source.id))

      render_change(view, :start_search, %{
        "search" => %{
          @default_search_params
          | "querystring" => "c:countd(event_message) c:group_by(t::hour)",
            "chart_aggregate" => "countd",
            "chart_period" => "hour"
        }
      })

      TestUtils.retry_assert(fn ->
        html = view |> element("#logs-list-container") |> render()
        assert html =~ "test message"
        assert_receive {:ok, :countd}
      end)
    end

    test "bug: top-level key with nested key filters", %{conn: conn, source: source} do
      # ref https://www.notion.so/supabase/Backend-Search-Error-187112eabd094dcc8042c6952f4f5fac

      GoogleApi.BigQuery.V2.Api.Tables
      |> stub(:bigquery_tables_patch, fn _conn, _proj, _dataset, _table, _opts ->
        {:ok, %GoogleApi.BigQuery.V2.Model.Table{}}
      end)

      le = build(:log_event, metadata: %{"nested" => "something"}, top: "level", source: source)
      :timer.sleep(100)

      # Backends.via_source(source, Schema, nil)
      Schema.handle_cast({:update, le, source}, %{
        source_id: source.id,
        source_token: source.token,
        bigquery_project_id: nil,
        bigquery_dataset_id: nil,
        field_count: 3,
        field_count_limit: 500,
        next_update: System.system_time(:millisecond)
      })

      Cachex.clear(Logflare.SourceSchemas.Cache)

      stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, opts ->
        query = opts[:body].query |> String.downcase()

        if query =~ "select" and query =~ "nested" do
          assert query =~ "0.top = ?"
          assert query =~ "1.nested = ?"
          {:ok, TestUtils.gen_bq_response(%{"event_message" => "some correct message"})}
        else
          {:ok, TestUtils.gen_bq_response()}
        end
      end)

      {:ok, view, _html} = live(conn, Routes.live_path(conn, SearchLV, source.id))
      %{executor_pid: search_executor_pid} = get_view_assigns(view)
      Ecto.Adapters.SQL.Sandbox.allow(Logflare.Repo, self(), search_executor_pid)

      view
      |> TestUtils.wait_for_render("#logs-list-container li")

      # post-init fetching
      view
      |> TestUtils.wait_for_render("#logs-list-container")

      TestUtils.retry_assert(fn ->
        render_change(view, :start_search, %{
          "search" => %{@default_search_params | "querystring" => "m.nested:test top:test"}
        })

        view
        |> TestUtils.wait_for_render("#logs-list-container li")

        html = view |> element("#logs-list-container") |> render()

        assert html =~ "some correct message"
      end)
    end

    test "chart display interval", %{conn: conn, source: source} do
      stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, opts ->
        params = opts[:body].queryParameters

        if length(params) > 2 do
          assert Enum.any?(params, fn param -> param.parameterValue.value == "MINUTE" end)
          # truncate by 120 minutes
          assert Enum.any?(params, fn param -> param.parameterValue.value == 120 end)
        end

        {:ok, TestUtils.gen_bq_response()}
      end)

      {:ok, view, _html} = live(conn, Routes.live_path(conn, SearchLV, source.id))

      %{executor_pid: search_executor_pid} = get_view_assigns(view)
      Ecto.Adapters.SQL.Sandbox.allow(Logflare.Repo, self(), search_executor_pid)

      # post-init fetching
      view
      |> TestUtils.wait_for_render("#logs-list-container li")

      render_change(view, :start_search, %{
        "search" => %{@default_search_params | "chart_period" => "day"}
      })

      # wait for async search task to complete
      view
      |> TestUtils.wait_for_render("#logs-list-container li")

      html = view |> element("#logs-list-container") |> render()
      assert html =~ "some event message"
    end

    test "date picker adjusts chart display interval", %{conn: conn, source: source} do
      query = "t:2025-08-01T00:00:00..2025-08-02T00:00:00"

      {:ok, view, _html} =
        live(conn, Routes.live_path(conn, SearchLV, source.id, tailing?: false))

      # Increasing the chart period
      assert view
             |> has_element?("#search_chart_period option[selected]", "minute")

      render_change(view, :datetime_update, %{
        "querystring" => query
      })

      assert view
             |> has_element?("#search_chart_period option[selected]", "hour")

      # Reducing the chart period
      render_change(view, :datetime_update, %{
        "querystring" => "t:last@15m"
      })

      assert view
             |> has_element?("#search_chart_period option[selected]", "second")

      # a chart period selected by the user is preserved, and search halted
      render_change(view, :form_update, %{
        "search" => %{
          @default_search_params
          | "querystring" => query,
            "chart_period" => "day"
        }
      })

      assert view
             |> has_element?(".alert", "Search halted")

      rendered = view |> render()

      assert rendered =~ "t%3Alast%4015minute"
      assert rendered =~ "c%3Acount%28%2A%29"
      assert rendered =~ "c%3Agroup_by%28t%3A%3Asecond%29"
      assert rendered =~ "tailing%3F=false"
      assert rendered =~ "Set chart period to second</a>"
    end

    test "log event links", %{conn: conn, source: source} do
      stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, _opts ->
        {:ok,
         TestUtils.gen_bq_response(%{
           "event_message" => "some modal message",
           "testing" => "modal123",
           "id" => "some-uuid"
         })}
      end)

      {:ok, view, _html} = live(conn, Routes.live_path(conn, SearchLV, source.id))

      %{executor_pid: search_executor_pid} = get_view_assigns(view)
      Ecto.Adapters.SQL.Sandbox.allow(Logflare.Repo, self(), search_executor_pid)

      # wait for async search task to complete
      view
      |> TestUtils.wait_for_render("#logs-list li:first-of-type a[href^='/sources']")

      assert view
             |> element("#logs-list li:first-of-type a[href^='/sources']", "permalink")
             |> render() =~ ~r/timestamp=\d{4}-\d{2}-\d{2}/

      link =
        view
        |> element(
          "#logs-list li:first-of-type a[phx-value-log-event-id='some-uuid']",
          "view"
        )
        |> render()

      assert link =~ ~r/phx-value-log-event-timestamp="\d+/
      assert link =~ ~r/phx-value-lql="\w+/
    end

    test "log event selected fields", %{conn: conn, source: source} do
      stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, _opts ->
        {:ok,
         TestUtils.gen_bq_response(%{
           "event_message" => "some event message",
           "testing" => "modal123",
           "user_id" => "user-abc-123",
           "id" => "some-uuid"
         })}
      end)

      {:ok, view, _html} =
        live(conn, ~p"/sources/#{source.id}/search?#{%{querystring: "s:user.id"}}")

      %{executor_pid: search_executor_pid} = get_view_assigns(view)
      Ecto.Adapters.SQL.Sandbox.allow(Logflare.Repo, self(), search_executor_pid)

      view
      |> TestUtils.wait_for_render("#logs-list")

      assert view |> element("#logs-list-container") |> render() =~
               "some event message"

      html = view |> element("#logs-list-container #log-some-uuid-selected-fields") |> render()
      assert html =~ "id"
      assert html =~ "user-abc-123"
    end

    test "log event modal", %{conn: conn, user: user} do
      schema = TestUtils.build_bq_schema(%{"testing" => "string"})
      source = insert(:source, user: user)
      insert(:source_schema, source: source, bigquery_schema: schema)
      # TODO: use expect, remove UDFs creation query
      stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, _opts ->
        {:ok,
         TestUtils.gen_bq_response(%{
           "event_message" => "some modal message",
           "testing" => "modal123",
           "id" => "some-uuid"
         })}
      end)

      {:ok, view, _html} =
        live(conn, ~p"/sources/#{source.id}/search?#{%{querystring: "testing:modal123"}}")

      %{executor_pid: search_executor_pid} = get_view_assigns(view)
      Ecto.Adapters.SQL.Sandbox.allow(Logflare.Repo, self(), search_executor_pid)

      # wait for async search task to complete
      view
      |> TestUtils.wait_for_render("li:first-of-type a[phx-value-log-event-id='some-uuid']")

      schema = TestUtils.build_bq_schema(%{"testing" => "string"})
      source = insert(:source, user: user)
      insert(:source_schema, source: source, bigquery_schema: schema)
      pid = self()

      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, opts ->
        query = opts[:body].query
        send(pid, {:query, query})

        {:ok,
         TestUtils.gen_bq_response(%{
           "event_message" => "some modal message",
           "testing" => "modal123",
           "id" => "some-uuid"
         })}
      end)

      TestUtils.retry_assert(fn ->
        view
        |> element("li:first-of-type a[phx-value-log-event-id='some-uuid']", "view")
        |> render_click()
      end)

      TestUtils.retry_assert(fn ->
        html = render(view)

        assert html =~ "Raw JSON"
        assert html =~ "modal123"
        assert html =~ "some modal message"
      end)

      assert_receive {:query, query}
      # filter on the field name
      assert query =~ ~r"..\.testing"
    end

    test "log event modal - quick filter button appends filter to search", %{
      conn: conn,
      source: source
    } do
      stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, _opts ->
        {:ok,
         TestUtils.gen_bq_response(%{
           "event_message" => "quick filter test",
           "user_id" => "abc-123",
           "id" => "qf-uuid"
         })}
      end)

      {:ok, view, _html} =
        live(conn, ~p"/sources/#{source.id}/search?#{%{querystring: ~s|user_id:"abc-123"|}}")

      %{executor_pid: search_executor_pid} = get_view_assigns(view)
      Ecto.Adapters.SQL.Sandbox.allow(Logflare.Repo, self(), search_executor_pid)

      view
      |> TestUtils.wait_for_render("#logs-list-container li")

      view
      |> element("li:first-of-type a[phx-value-log-event-id='qf-uuid']", "view")
      |> render_click()

      TestUtils.retry_assert(fn ->
        html = render(view)
        assert html =~ "quick filter test"
        assert html =~ ~s|title="Append to query"|
      end)

      view
      |> element(~s|#log-event-tree-qf-uuid--user_id a[title="Append to query"]|)
      |> render_click()

      assert_patch(view)
      assert find_querystring(render(view)) =~ ~s|user_id:"abc-123"|
    end

    test "log event modal - loading from cache", %{conn: conn, user: user} do
      schema = TestUtils.build_bq_schema(%{"testing" => "string"})
      source = insert(:source, user: user)
      insert(:source_schema, source: source, bigquery_schema: schema)

      stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, _opts ->
        {:ok,
         TestUtils.gen_bq_response(%{
           "event_message" => "some modal message",
           "testing" => "modal123",
           "id" => "some-uuid"
         })}
      end)

      {:ok, view, _html} =
        live(conn, ~p"/sources/#{source.id}/search?#{%{querystring: ""}}")

      %{executor_pid: search_executor_pid} = get_view_assigns(view)
      Ecto.Adapters.SQL.Sandbox.allow(Logflare.Repo, self(), search_executor_pid)

      # Wait for search to complete
      view
      |> TestUtils.wait_for_render("li:first-of-type a[phx-value-log-event-id='some-uuid']")

      # First render builds a LogEvent and caches it
      view
      |> element("li:first-of-type a[phx-value-log-event-id='some-uuid']", "view")
      |> render_click()

      # wait for cache to populate
      view
      |> TestUtils.wait_for_render("#logflare-modal")

      # Second render loads the LogEvent from cache
      assert view
             |> element("li:first-of-type a[phx-value-log-event-id='some-uuid']", "view")
             |> render_click()
    end

    test "shows flash error for malformed query", %{conn: conn, source: source} do
      assert {:ok, view, _html} =
               live(conn, Routes.live_path(conn, SearchLV, source, querystring: "t:20022"))

      %{executor_pid: search_executor_pid} = get_view_assigns(view)
      Ecto.Adapters.SQL.Sandbox.allow(Logflare.Repo, self(), search_executor_pid)

      assert render(view) =~ "Error while parsing timestamp filter"
    end

    test "shows flash error for query exceeding processed bytes limit", %{
      conn: conn,
      source: source
    } do
      assert {:ok, view, _html} =
               live(conn, Routes.live_path(conn, SearchLV, source, querystring: "t:20022"))

      %{executor_pid: search_executor_pid} = get_view_assigns(view)
      Ecto.Adapters.SQL.Sandbox.allow(Logflare.Repo, self(), search_executor_pid)

      error_response =
        %{
          error: %{
            message:
              "Query exceeded limit for bytes billed: 2000000000. 20004857600 or higher required."
          }
        }
        |> Jason.encode!()

      send(view.pid, {:search_error, %{error: %Tesla.Env{status: 400, body: error_response}}})

      assert render(view) =~
               "Query halted: total bytes processed for this query is expected to be greater than 2 GB"
    end

    test "redirected for non-owner user", %{conn: conn, source: source} do
      non_owner_user = insert(:user)

      conn =
        conn
        |> login_user(non_owner_user)
        |> get(Routes.live_path(conn, SearchLV, source))

      assert html_response(conn, 404) =~ "not found"
    end

    test "redirected for anonymous user", %{conn: conn, source: source} do
      conn =
        conn
        |> Map.update!(:private, &Map.drop(&1, [:plug_session]))
        |> Plug.Test.init_test_session(%{})
        |> assign(:user, nil)
        |> get(Routes.live_path(conn, SearchLV, source))

      assert Phoenix.Flash.get(conn.assigns.flash, :error) =~ "must be logged in"
      assert html_response(conn, 302)
      assert redirected_to(conn) == "/auth/login"
    end

    test "stop/start live search", %{conn: conn, source: source} do
      {:ok, view, _html} = live(conn, Routes.live_path(conn, SearchLV, source))
      %{executor_pid: search_executor_pid} = view |> get_view_assigns()
      Ecto.Adapters.SQL.Sandbox.allow(Logflare.Repo, self(), search_executor_pid)

      # post-init fetching
      view
      |> TestUtils.wait_for_render("#logs-list-container li")

      assert get_view_assigns(view).tailing?
      render_click(view, "soft_pause", %{})

      # Allow database access after pause click which might trigger a new search
      %{executor_pid: search_executor_pid} = view |> get_view_assigns()
      Ecto.Adapters.SQL.Sandbox.allow(Logflare.Repo, self(), search_executor_pid)

      refute get_view_assigns(view).tailing?

      render_click(view, "soft_play", %{})

      # Allow database access after play click which might trigger a new search
      %{executor_pid: search_executor_pid} = view |> get_view_assigns()
      Ecto.Adapters.SQL.Sandbox.allow(Logflare.Repo, self(), search_executor_pid)

      assert get_view_assigns(view).tailing?
    end

    test "datetime_update", %{conn: conn, source: source} do
      {:ok, view, _html} =
        live(conn, Routes.live_path(conn, SearchLV, source, querystring: "error"))

      %{executor_pid: search_executor_pid} = get_view_assigns(view)
      Ecto.Adapters.SQL.Sandbox.allow(Logflare.Repo, self(), search_executor_pid)

      # post-init fetching
      view
      |> TestUtils.wait_for_render("#logs-list-container")

      render_change(view, "datetime_update", %{"querystring" => "t:last@2h"})

      assert get_view_assigns(view).querystring =~ "t:last@2hour"
      assert get_view_assigns(view).querystring =~ "error"

      render_change(view, "datetime_update", %{
        "querystring" => "t:2020-04-20T00:{01..02}:00",
        "period" => "second"
      })

      assert get_view_assigns(view).querystring =~ "error"
      assert get_view_assigns(view).querystring =~ "t:2020-04-20T00:{01..02}:00"
    end
  end

  describe "create from query" do
    setup do
      %{user: user} = team = insert(:team)

      team_user =
        insert(:team_user, team: team, preferences: build(:user_preferences, timezone: "NZ"))

      source = insert(:source, user: user)
      plan = insert(:plan)
      [user: user, source: source, plan: plan, team_user: team_user]
    end

    setup [:setup_team_user_session]

    test "create new query from search", %{conn: conn, source: source, team_user: team_user} do
      {:ok, view, _html} =
        live(
          conn,
          ~p"/sources/#{source.id}/search?t=#{team_user.team_id}&querystring=something123&tailing%3F=&tz=Etc/UTC"
        )

      # Wait until search has executed
      view
      |> TestUtils.wait_for_render("#create-menu button:not([disabled])")

      view
      |> element(~s|a[phx-value-resource="query"]|, "From search")
      |> render_click()

      {redirect_path, _flash} = assert_redirect(view)
      assert redirect_path =~ "/query?q=SELECT"
      assert redirect_path =~ "something123"
      assert redirect_path =~ source.name
    end

    test "create new alert, endpoint from search", %{
      conn: conn,
      source: source,
      team_user: team_user
    } do
      ["alert", "endpoint"]
      |> Enum.each(fn resource ->
        {:ok, view, _html} =
          live(
            conn,
            ~p"/sources/#{source.id}/search?t=#{team_user.team_id}&querystring=something123&tailing%3F=&tz=Etc/UTC"
          )

        view
        |> TestUtils.wait_for_render("#create-menu button:not([disabled])")

        view
        |> element(~s|a[phx-value-resource="#{resource}"]|, "From search")
        |> render_click()

        {redirect_path, _flash} = assert_redirect(view)
        assert redirect_path =~ "/#{resource}s/new"

        %{"query" => query, "name" => name} =
          URI.new!(redirect_path) |> Map.get(:query) |> URI.decode_query()

        assert query =~ "SELECT t0.timestamp, t0.id, t0.event_message FROM `#{source.name}`"
        assert query =~ "something123"
        assert query =~ source.name
        assert name == source.name
      end)
    end

    test "create new query from chart query", %{conn: conn, source: source, team_user: team_user} do
      {:ok, view, _html} =
        live(
          conn,
          ~p"/sources/#{source.id}/search?t=#{team_user.team_id}&querystring=something123&tailing%3F=&tz=Etc/UTC"
        )

      view
      |> TestUtils.wait_for_render("#create-menu button:not([disabled])")

      view
      |> element(~s|a[phx-value-resource="query"]|, "From chart")
      |> render_click()

      {redirect_path, _flash} = assert_redirect(view)
      %{"q" => query} = URI.new!(redirect_path) |> Map.get(:query) |> URI.decode_query()

      assert query =~ "SELECT (case\nwhen 'MINUTE' = 'DAY'"
      assert query =~ "something123"
      assert query =~ source.name
    end

    test "create new alert, endpoint from chart", %{
      conn: conn,
      source: source,
      team_user: team_user
    } do
      ["alert", "endpoint"]
      |> Enum.each(fn resource ->
        {:ok, view, _html} =
          live(
            conn,
            ~p"/sources/#{source.id}/search?t=#{team_user.team_id}&querystring=something123&tailing%3F=&tz=Etc/UTC"
          )

        view
        |> TestUtils.wait_for_render("#create-menu button:not([disabled])")

        view
        |> element(~s|a[phx-value-resource="#{resource}"]|, "From chart")
        |> render_click()

        {redirect_path, _flash} = assert_redirect(view)
        assert redirect_path =~ "/#{resource}s/new"

        %{"query" => query, "name" => name} =
          URI.new!(redirect_path) |> Map.get(:query) |> URI.decode_query()

        assert query =~ "SELECT (case\nwhen 'MINUTE' = 'DAY'"
        assert query =~ "something123"
        assert query =~ source.name
        assert name == source.name
      end)
    end
  end

  describe "single tenant searching" do
    TestUtils.setup_single_tenant(seed_user: true)

    setup do
      user = SingleTenant.get_default_user()
      source = insert(:source, user: user)
      plan = SingleTenant.get_default_plan()
      [user: user, source: source, plan: plan]
    end

    setup [:setup_user_session]

    test "run a query", %{conn: conn, source: source} do
      stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn conn, _proj_id, opts ->
        # use separate connection pool
        assert {Tesla.Adapter.Finch, :call, [[name: Logflare.FinchQuery, receive_timeout: _]]} =
                 conn.adapter

        query = opts[:body].query |> String.downcase()

        if query =~ "strpos(t0.event_message, ?" do
          {:ok,
           TestUtils.gen_bq_response(%{
             "event_message" => "some correct message",
             "level" => "warning"
           })}
        else
          {:ok, TestUtils.gen_bq_response()}
        end
      end)

      {:ok, view, _html} = live(conn, Routes.live_path(conn, SearchLV, source.id))

      %{executor_pid: search_executor_pid} = view |> get_view_assigns()
      Ecto.Adapters.SQL.Sandbox.allow(Logflare.Repo, self(), search_executor_pid)

      # post-init fetching

      render_change(view, :start_search, %{
        "search" => %{@default_search_params | "querystring" => "somestring"}
      })

      TestUtils.retry_assert(fn ->
        html = view |> element("#logs-list-container") |> render()

        assert html =~ "some correct message"
        assert html =~ ~s|class="log-level-warning">warning|
      end)
    end
  end

  describe "source suggestion fields handling" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user, suggested_keys: "event_message")
      source_without_suggestion = insert(:source, user: user)
      plan = insert(:plan)

      %{
        user: user,
        source: source,
        plan: plan,
        source_without_suggestion: source_without_suggestion
      }
    end

    setup [:setup_user_session]

    test "on source with suggestion fields, creates flash with link to force query", %{
      conn: conn,
      source: source
    } do
      {:ok, view, _html} = live(conn, Routes.live_path(conn, SearchLV, source.id))

      view
      |> TestUtils.wait_for_render("#logs-list-container")

      view
      |> render_change(:start_search, %{
        "search" => %{
          @default_search_params
          | "querystring" => "c:count(*) c:group_by(t::minute)"
        }
      })

      flash = view |> element(".message .alert") |> render()
      assert flash =~ "Query does not include suggested keys"
      assert flash =~ "event_message"
      assert flash =~ "Click to force query"
      assert flash =~ "force=true"
    end

    test "on source with suggestion fields, does not create a flash when query includes field", %{
      conn: conn,
      source: source
    } do
      {:ok, view, _html} = live(conn, Routes.live_path(conn, SearchLV, source.id))

      view
      |> TestUtils.wait_for_render("#logs-list-container")

      view
      |> render_change(:start_search, %{
        "search" => %{
          @default_search_params
          | "querystring" => "c:count(*) c:group_by(t::minute) message"
        }
      })

      refute view |> element(".message .alert") |> has_element?()
    end

    test "on source without suggestion fields, does not create a flash", %{
      conn: conn,
      source_without_suggestion: source
    } do
      {:ok, view, _html} = live(conn, Routes.live_path(conn, SearchLV, source.id))

      view
      |> TestUtils.wait_for_render("#logs-list-container li")

      assert view
             |> render_change(:start_search, %{
               "search" => %{
                 @default_search_params
                 | "querystring" => "c:count(*) c:group_by(t::minute) message"
               }
             })
             |> Floki.parse_document!()
             |> Floki.find("div[role=alert]>span")
             |> Enum.empty?()
    end
  end

  describe "source suggestion required fields" do
    setup do
      plan = insert(:plan)
      user = insert(:user)
      source = insert(:source, user: user, suggested_keys: "metadata.level!")
      %{user: user, plan: plan, source: source}
    end

    setup [:setup_user_session]

    test "on source with suggestion fields, creates flash with link to force query", %{
      conn: conn,
      source: source
    } do
      {:ok, view, _html} = live(conn, Routes.live_path(conn, SearchLV, source.id))

      view
      |> TestUtils.wait_for_render("#logs-list-container")

      view
      |> render_change(:start_search, %{
        "search" => %{
          @default_search_params
          | "querystring" => "c:count(*) c:group_by(t::minute)"
        }
      })

      flash = view |> element(".message .alert") |> render()
      assert flash =~ "Query does not include required keys"
      assert flash =~ "metadata.level"
      refute flash =~ "Click to force query"
      refute flash =~ "force=true"
    end

    test "on source with required fields, does not create a flash when query includes field", %{
      conn: conn,
      source: source
    } do
      {:ok, view, _html} = live(conn, Routes.live_path(conn, SearchLV, source.id))

      view
      |> TestUtils.wait_for_render("#logs-list-container")

      view
      |> render_change(:start_search, %{
        "search" => %{
          @default_search_params
          | "querystring" => "c:count(*) c:group_by(t::minute) metadata.level:error"
        }
      })

      refute view |> element(".message .alert", "required") |> has_element?()
    end
  end

  describe "recommended field inputs" do
    setup do
      plan = insert(:plan)
      user = insert(:user)

      source =
        insert(:source,
          user: user,
          suggested_keys: "metadata.level!,m.user_id",
          bigquery_clustering_fields: "session_id"
        )

      source_without_recommendations = insert(:source, user: user)

      %{
        user: user,
        plan: plan,
        source: source,
        source_without_recommendations: source_without_recommendations
      }
    end

    setup [:setup_user_session]

    test "renders inputs for suggested and cluster fields", %{conn: conn, source: source} do
      {:ok, view, _html} = live(conn, Routes.live_path(conn, SearchLV, source.id))

      assert has_element?(view, "label", "session_id")
      assert has_element?(view, "label", "metadata.level")
      assert has_element?(view, "label", "m.user_id")
      assert has_element?(view, ".required-field-indicator", "required")
    end

    test "does not render inputs when no fields are configured", %{
      conn: conn,
      source_without_recommendations: source
    } do
      {:ok, view, _html} = live(conn, Routes.live_path(conn, SearchLV, source.id))

      refute has_element?(view, "input[id^='search-field-']")
    end

    # Search initiated from SourceController.show
    test "search with field params are appended", %{
      conn: conn,
      source: source
    } do
      query_params = [
        {"fields[metadata.level]", "error"},
        {"fields[metadata.user_id]", "123"},
        {"fields[event_message]", "api-timeout"},
        {"fields[metadata.request_id]", ""},
        querystring: "c:count(*) c:group_by(t::minute)"
      ]

      path = ~p"/sources/#{source.id}/search?#{query_params}"

      {:error, {:live_redirect, %{to: to}}} =
        live(conn, path)

      refute to =~ "fields%5B"

      {:ok, view, _html} = live(conn, to)

      qs = render(view) |> find_querystring()

      assert qs =~ "api-timeout"
      assert qs =~ "m.level:error"
      assert qs =~ "m.user_id:123"
      refute qs =~ "m.request_id:"
    end

    test "start_search upserts filters by path and ignores empty fields", %{
      conn: conn,
      source: source
    } do
      {:ok, view, _html} = live(conn, Routes.live_path(conn, SearchLV, source.id))

      %{executor_pid: search_executor_pid} = get_view_assigns(view)
      Ecto.Adapters.SQL.Sandbox.allow(Logflare.Repo, self(), search_executor_pid)

      render_change(view, :start_search, %{
        "search" => %{
          @default_search_params
          | "querystring" => "event_message:timeout c:count(*) c:group_by(t::minute)"
        },
        "fields" => %{
          "event_message" => "api-timeout",
          "metadata.request_id" => ""
        }
      })

      qs = render(view) |> find_querystring()

      assert qs =~ "api-timeout"
      refute qs =~ "event_message:timeout"
      refute qs =~ "m.request_id:"
    end
  end

  describe "source disable tailing" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user, disable_tailing: true)
      plan = insert(:plan)

      %{
        user: user,
        source: source,
        plan: plan
      }
    end

    setup [:setup_user_session]

    test "on source load, do not auto-tail", %{
      conn: conn,
      source: source
    } do
      # only two query runs each, one for logs list, one for chart
      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 4, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response()}
      end)

      {:ok, view, _html} =
        live(conn, ~p"/sources/#{source.id}/search?querystring=something123&tailing%3F=true")

      refute get_view_assigns(view).tailing?

      view
      |> TestUtils.wait_for_render("#logs-list-container")

      assert render_click(view, "soft_play", %{}) =~ "disabled for this source"

      view
      |> render_change(:start_search, %{
        "search" => %{
          @default_search_params
          | "querystring" => "c:count(*) c:group_by(t::minute) message",
            "tailing?" => "true"
        }
      })

      refute get_view_assigns(view).tailing?

      view
      |> TestUtils.wait_for_render("#logs-list-container")
    end
  end

  describe "timezone dropdown behavior" do
    setup do
      user = insert(:user, %{preferences: %{timezone: "Singapore"}})
      source = insert(:source, user: user)
      plan = insert(:plan)
      [user: user, source: source, plan: plan]
    end

    setup [:setup_user_session]

    test "when remember is checked timezone is set in user preferences",
         %{
           conn: conn,
           user: user,
           source: source
         } do
      {:ok, view, _html} =
        live(
          conn,
          ~p"/sources/#{source.id}/search?querystring=something123&tailing%3F=&tz=Singapore"
        )

      view
      |> element("#results-actions")
      |> render_change(%{
        search_timezone: "Europe/London",
        remember_timezone: "true"
      })

      assert view
             |> has_element?("#results-actions_search_timezone :checked", "Europe/London")

      assert Logflare.Repo.reload!(user).preferences.timezone == "Europe/London"
    end

    test "remember checkbox is hidden when search_timezone is same as user's timezone", %{
      conn: conn,
      source: source
    } do
      {:ok, view, _html} =
        live(
          conn,
          ~p"/sources/#{source.id}/search?querystring=something123&tailing%3F=&tz=Singapore"
        )

      assert view
             |> element("#results-actions")
             |> render_change(%{timezone: "Singapore"})

      html = render(view)

      # Checkbox should be hidden when display_timezone == user's timezone
      refute html =~ "name=\"remember_timezone\""
      refute html =~ "Remember"
    end

    test "when search_timezone is different to user's timezone, show checkbox", %{
      conn: conn,
      user: user,
      source: source
    } do
      {:ok, view, _html} =
        live(
          conn,
          ~p"/sources/#{source.id}/search?querystring=something123&tailing%3F=&tz=Singapore"
        )

      assert user.preferences.timezone == "Singapore"

      assert view
             |> element("#results-actions")
             |> render_change(%{search_timezone: "America/New_York"})

      html = render(view)

      assert html =~ "name=\"remember_timezone\""
      assert html =~ "Remember"
    end

    test "bug: switching tz should not result in query getting cleared", %{
      conn: conn,
      source: source
    } do
      {:ok, view, _html} =
        live(
          conn,
          ~p"/sources/#{source.id}/search?querystring=something123&tailing%3F=&tz=Singapore"
        )

      # switch to arizona
      assert view
             |> element("#results-actions")
             |> render_change(%{search_timezone: "US/Arizona"})

      to = assert_patch(view)

      assert to =~ "something123"
      assert to =~ "US%2FArizona"

      # don't follow_redirect because we alter cookies (for team users), load the redireted url directly
      {:ok, view, _html} = live(conn, to)

      assert render(view) =~ "something123"
      assert view |> element(".subhead") |> render() =~ "(-07:00)"
    end
  end

  describe "bigquery reservation search" do
    setup do
      plan = insert(:plan)
      [plan: plan]
    end

    test "when bigquery reservation search is not set, do not set reservation option", ctx do
      user = insert(:user, bigquery_reservation_search: nil)
      %{conn: conn} = setup_user_session(Map.put(ctx, :user, user)) |> Map.new()
      source = insert(:source, user: user)
      pid = self()

      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, opts ->
        reservation = opts[:body].reservation
        send(pid, {:reservation, reservation})

        {:ok,
         TestUtils.gen_bq_response(%{
           "event_message" => "some modal message",
           "testing" => "modal123",
           "id" => "some-uuid"
         })}
      end)

      {:ok, _view, _html} =
        live(
          conn,
          ~p"/sources/#{source.id}/search?querystring=something123&tailing%3F=false"
        )

      assert_receive {:reservation, nil}
    end

    test "when bigquery reservation search is set, set job option to reservation", ctx do
      user =
        insert(:user, bigquery_reservation_search: "projects/1234567890/reservations/1234567890")

      %{conn: conn} = setup_user_session(Map.put(ctx, :user, user)) |> Map.new()

      source = insert(:source, user: user)
      pid = self()

      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, opts ->
        reservation = opts[:body].reservation
        send(pid, {:reservation, reservation})

        {:ok,
         TestUtils.gen_bq_response(%{
           "event_message" => "some modal message",
           "testing" => "modal123",
           "id" => "some-uuid"
         })}
      end)

      {:ok, _view, _html} =
        live(
          conn,
          ~p"/sources/#{source.id}/search?querystring=something123&tailing%3F=false"
        )

      assert_receive {:reservation, reservation}
      assert reservation == user.bigquery_reservation_search
    end
  end

  describe "team query param preservation in search page" do
    setup [:on_exit_kill_tasks, :setup_mocks]

    setup %{conn: conn} do
      plan = insert(:plan)
      user = insert(:user)
      source = insert(:source, user: user)
      team = insert(:team, user: user)
      team_user = insert(:team_user, team: team, email: user.email)

      _billing_account = insert(:billing_account, user: user, stripe_plan_id: plan.stripe_id)
      user = user |> Logflare.Repo.preload(:billing_account)
      conn = login_user(conn, user, team_user)

      [team: team, team_user: team_user, conn: conn, source: source, user: user, plan: plan]
    end

    test "search page links preserve team param", %{
      conn: conn,
      source: source,
      team_user: team_user
    } do
      {:ok, _view, html} =
        live(
          conn,
          ~p"/sources/#{source}/search?querystring=test&tailing%3F=false&t=#{team_user.team_id}"
        )

      for path <- ["sources/#{source.id}"] do
        assert html =~ ~r/#{path}[^"<]*t=#{team_user.team_id}/
      end
    end

    test "search page without t= param assigns team context and preserves it in links", %{
      conn: conn,
      source: source,
      team_user: team_user
    } do
      {:ok, view, html} =
        live(
          conn,
          ~p"/sources/#{source}/search?querystring=test&tailing%3F=false"
        )

      assert html =~ source.name
      assert view |> has_element?(~s|a[href="/sources/#{source.id}?t=#{team_user.team_id}"]|)
    end
  end

  defp get_view_assigns(view) do
    :sys.get_state(view.pid).socket.assigns
  end

  defp find_search_form_value(html, selector) do
    {:ok, document} = Floki.parse_document(html)

    document
    |> Floki.find(selector)
    |> Floki.attribute("value")
    |> hd
  end

  def find_selected_chart_period(html) do
    find_search_form_value(html, "#search_chart_period option[selected]")
  end

  def find_selected_chart_aggregate(html) do
    assert find_search_form_value(html, "#search_chart_aggregate option[selected]")
  end

  def find_chart_aggregate(html) do
    assert find_search_form_value(html, "#search_chart_aggregate option")
  end

  def find_querystring(html) do
    find_search_form_value(html, "#search_querystring")
  end
end
