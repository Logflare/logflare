defmodule LogflareWeb.Source.SearchLVTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  alias Logflare.Source
  alias Logflare.SingleTenant
  alias Logflare.Source.BigQuery.Schema
  alias LogflareWeb.Source.SearchLV
  alias Logflare.Backends
  alias Logflare.Source.V1SourceSup
  alias Logflare.SystemMetrics.AllLogsLogged

  import Phoenix.LiveViewTest

  @endpoint LogflareWeb.Endpoint
  @default_search_params %{
    "querystring" => "c:count(*) c:group_by(t::minute)",
    "chart_period" => "minute",
    "chart_aggregate" => "count",
    "tailing?" => "false"
  }

  defp setup_mocks(_ctx) do
    stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, _opts ->
      {:ok, TestUtils.gen_bq_response()}
    end)

    :ok
  end

  defp on_exit_kill_tasks(_ctx) do
    on_exit(fn -> Logflare.Utils.Tasks.kill_all_tasks() end)

    :ok
  end

  # requires a source, and plan set
  defp setup_source_processes(context) do
    start_supervised!(AllLogsLogged)

    Enum.each(context, fn
      {_, %Source{} = source} ->
        start_supervised!({V1SourceSup, source: source}, id: source.token)

      _ ->
        nil
    end)

    :ok
  end

  # to simulate signed in user.
  defp setup_user_session(%{conn: conn, user: user, plan: plan}) do
    _billing_account = insert(:billing_account, user: user, stripe_plan_id: plan.stripe_id)
    user = user |> Logflare.Repo.preload(:billing_account)
    conn = conn |> put_session(:user_id, user.id) |> assign(:user, user)
    [conn: conn]
  end

  defp setup_team_user_session(%{conn: conn, user: user, plan: plan, team_user: team_user}) do
    _billing_account = insert(:billing_account, user: user, stripe_plan_id: plan.stripe_id)
    user = user |> Logflare.Repo.preload(:billing_account)

    conn =
      conn
      |> put_session(:team_user_id, team_user.id)
      |> put_session(:user_id, user.id)
      |> assign(:team_user, team_user)

    [conn: conn]
  end

  # do this for all tests
  setup [:setup_mocks, :on_exit_kill_tasks]

  describe "no timezone preference for user" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)
      plan = insert(:plan)
      [user: user, source: source, plan: plan]
    end

    setup [:setup_user_session, :setup_source_processes]

    test "subheader - default timezone is Etc/UTC", %{conn: conn, source: source} do
      {:ok, view, _html} = live(conn, ~p"/sources/#{source.id}/search")

      assert view
             |> element(".subhead a", "timezone")
             |> render_click()

      :timer.sleep(300)
      assert render(view) =~ "local timezone for your"
      # default to Etc/UTC
      assert view |> element(".subhead") |> render() =~ "(+00:00)"
    end

    test "subheader - local time toggle", %{conn: conn, source: source} do
      {:ok, view, _html} = live(conn, ~p"/sources/#{source.id}/search")

      assert view
             |> element(".subhead a", "local time")
             |> render_click()

      :timer.sleep(200)
      assert element(view, ".subhead a .toggle-on")
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

    setup [:setup_user_session, :setup_source_processes]

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

    test "bug: switching tz should not result in query getting cleared", %{
      conn: conn,
      source: source
    } do
      {:ok, view, _html} =
        live(
          conn,
          ~p"/sources/#{source.id}/search?querystring=something123&tailing%3F=&tz=Singapore"
        )

      assert view |> element(".subhead a", "timezone") |> render_click() =~ "Preferences"

      # switch to arizona
      {:error, {:live_redirect, %{to: to}}} =
        view
        |> element("form#user-tz-form")
        |> render_submit(%{user_preferences: %{timezone: "US/Arizona"}})

      assert to =~ "something123"
      assert to =~ "US%2FArizona"

      # don't follow_redirect because we alter cookies (for team users), load the redireted url directly
      {:ok, view, _html} = live(conn, to)

      assert render(view) =~ "something123"
      assert view |> element(".subhead") |> render() =~ "(-07:00)"
    end
  end

  describe "preference timezone for team_user" do
    setup do
      user = insert(:user)
      team_user = insert(:team_user, preferences: build(:user_preferences, timezone: "NZ"))
      source = insert(:source, user: user)
      plan = insert(:plan)
      [user: user, source: source, plan: plan, team_user: team_user]
    end

    setup [:setup_team_user_session, :setup_source_processes]

    test "subheader - if no tz, will redirect to preference tz", %{conn: conn, source: source} do
      {:error, {:live_redirect, %{to: to}}} =
        live(conn, ~p"/sources/#{source.id}/search?querystring=something123&tailing%3F=")

      assert to =~ "tz=NZ"
      assert to =~ "something123"
    end

    test "subheader - if ?tz=, will use param tz", %{conn: conn, source: source} do
      {:ok, view, _html} =
        live(
          conn,
          ~p"/sources/#{source.id}/search?querystring=something123&tailing%3F=&tz=Singapore"
        )

      :timer.sleep(300)

      assert view |> element(".subhead") |> render() =~ "(+08:00)"
      assert render(view) =~ "something123"
    end
  end

  describe "search tasks" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)
      plan = insert(:plan)
      [user: user, source: source, plan: plan]
    end

    setup [:setup_user_session, :setup_source_processes]

    test "subheader - lql docs", %{conn: conn, source: source} do
      {:ok, view, _html} = live(conn, ~p"/sources/#{source.id}/search")

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

      :timer.sleep(300)
      assert render(view) =~ "Actual SQL query used when querying for results"
    end

    test "subheader - aggregeate", %{conn: conn, source: source} do
      {:ok, view, _html} = live(conn, ~p"/sources/#{source.id}/search")

      assert view
             |> element(".subhead a", "aggregate")
             |> render_click()

      :timer.sleep(300)
      assert render(view) =~ "Actual SQL query used when querying for results"
    end

    test "load page", %{conn: conn, source: source} do
      {:ok, view, html} = live(conn, Routes.live_path(conn, SearchLV, source.id))

      assert html =~ "~/logs/"
      assert html =~ source.name
      assert html =~ "/search"

      # wait for async search task to complete
      :timer.sleep(1000)
      html = view |> element("#logs-list-container") |> render()
      assert html =~ "some event message"

      html = render(view)
      assert html =~ "Elapsed since last query"

      assert view
             |> element("#logs-list-container a", "permalink")
             |> has_element?()

      # permalink should have timestamp query parameter
      assert view
             |> element("#logs-list-container a", ~r/permalink/)

      assert view
             |> element("#logs-list-container a[href*='timestamp']", "permalink")
             |> has_element?()

      assert view
             |> element("#logs-list-container a[href*='uuid']", "permalink")
             |> has_element?()

      # default input values
      assert find_selected_chart_period(html) == "minute"
      assert find_chart_aggregate(html) == "count"
      assert find_querystring(html) == "c:count(*) c:group_by(t::minute)"
    end

    test "lql filters", %{conn: conn, source: source} do
      {:ok, view, _html} = live(conn, Routes.live_path(conn, SearchLV, source.id))

      :timer.sleep(1000)

      html = view |> element("#logs-list-container") |> render()
      assert html =~ "some event message"

      stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, opts ->
        params = opts[:body].queryParameters

        if length(params) > 2 do
          assert Enum.any?(params, fn param -> param.parameterValue.value == "crasher" end)
          assert Enum.any?(params, fn param -> param.parameterValue.value == "error" end)
        end

        {:ok, TestUtils.gen_bq_response(%{"event_message" => "some error message"})}
      end)

      render_change(view, :form_update, %{
        "search" => %{
          @default_search_params
          | "querystring" => "c:count(*) c:group_by(t::minute) error crasher"
        }
      })

      :timer.sleep(1000)

      render_change(view, :start_search, %{
        "search" => %{
          "querystring" => "c:count(*) c:group_by(t::minute) error crasher"
        }
      })

      # wait for async search task to complete
      :timer.sleep(1000)

      html = view |> element("#logs-list-container") |> render()

      assert html =~ "some error message"
      refute html =~ "some event message"
    end

    test "bug: top-level key with nested key filters", %{conn: conn, source: source} do
      # ref https://www.notion.so/supabase/Backend-Search-Error-187112eabd094dcc8042c6952f4f5fac

      GoogleApi.BigQuery.V2.Api.Tables
      |> stub(:bigquery_tables_patch, fn _conn, _proj, _dataset, _table, _opts ->
        {:ok, %GoogleApi.BigQuery.V2.Model.Table{}}
      end)

      le = build(:log_event, metadata: %{"nested" => "something"}, top: "level", source: source)
      :timer.sleep(100)

      Backends.via_source(source, Schema, nil)
      |> Schema.update(le)

      # TODO: find a better way to test a source schema structure

      stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, opts ->
        query = opts[:body].query |> String.downcase()

        if query =~ "select" and query =~ "inner join unnest" do
          assert query =~ "0.top = ?"
          assert query =~ "1.nested = ?"
          {:ok, TestUtils.gen_bq_response(%{"event_message" => "some correct message"})}
        else
          {:ok, TestUtils.gen_bq_response()}
        end
      end)

      {:ok, view, _html} = live(conn, Routes.live_path(conn, SearchLV, source.id))
      # post-init fetching
      :timer.sleep(800)

      render_change(view, :start_search, %{
        "search" => %{@default_search_params | "querystring" => "m.nested:test top:test"}
      })

      # wait for async search task to complete
      # TODO: find better way to test searching
      :timer.sleep(800)

      html = view |> element("#logs-list-container") |> render()

      assert html =~ "some correct message"
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
      # post-init fetching
      :timer.sleep(500)

      render_change(view, :start_search, %{
        "search" => %{@default_search_params | "chart_period" => "day"}
      })

      # wait for async search task to complete
      :timer.sleep(500)

      html = view |> element("#logs-list-container") |> render()
      assert html =~ "some event message"
    end

    test "log event modal", %{conn: conn, source: source} do
      stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, _opts ->
        {:ok,
         TestUtils.gen_bq_response(%{
           "event_message" => "some modal message",
           "testing" => "modal123",
           "id" => "some-uuid"
         })}
      end)

      {:ok, view, _html} = live(conn, Routes.live_path(conn, SearchLV, source.id))

      # wait for async search task to complete
      :timer.sleep(500)

      TestUtils.retry_assert(fn ->
        view
        |> element("li a[phx-value-log-event-id='some-uuid']", "view")
        |> render_click()
      end)

      TestUtils.retry_assert(fn ->
        html = render(view)

        assert html =~ "Raw JSON"
        assert html =~ "modal123"
        assert html =~ "some modal message"
      end)
    end

    test "shows flash error for malformed query", %{conn: conn, source: source} do
      assert {:ok, view, _html} =
               live(conn, Routes.live_path(conn, SearchLV, source, querystring: "t:20022"))

      assert render(view) =~ "Error while parsing timestamp filter"
    end

    test "redirected for non-owner user", %{conn: conn, source: source} do
      conn =
        conn
        |> assign(:user, insert(:user))
        |> get(Routes.live_path(conn, SearchLV, source))

      assert html_response(conn, 403) =~ "Forbidden"
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
      # post-init fetching
      :timer.sleep(500)

      assert get_view_assigns(view).tailing?
      render_click(view, "soft_pause", %{})
      refute get_view_assigns(view).tailing?

      render_click(view, "soft_play", %{})
      assert get_view_assigns(view).tailing?
    end

    test "datetime_update", %{conn: conn, source: source} do
      {:ok, view, _html} =
        live(conn, Routes.live_path(conn, SearchLV, source, querystring: "error"))

      # post-init fetching
      :timer.sleep(500)

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

  @tag :skip
  describe "single tenant searching" do
    TestUtils.setup_single_tenant(seed_user: true)

    setup do
      user = SingleTenant.get_default_user()
      source = insert(:source, user: user)
      plan = SingleTenant.get_default_plan()
      [user: user, source: source, plan: plan]
    end

    setup [:setup_user_session, :setup_source_processes]

    test "run a query", %{conn: conn, source: source} do
      stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn conn, _proj_id, opts ->
        # use separate connection pool
        assert {Tesla.Adapter.Finch, :call, [[name: Logflare.FinchQuery, receive_timeout: _]]} =
                 conn.adapter

        query = opts[:body].query |> String.downcase()

        if query =~ "strpos(t0.event_message, ?" do
          {:ok, TestUtils.gen_bq_response(%{"event_message" => "some correct message"})}
        else
          {:ok, TestUtils.gen_bq_response()}
        end
      end)

      {:ok, view, _html} = live(conn, Routes.live_path(conn, SearchLV, source.id))
      # post-init fetching
      :timer.sleep(800)

      render_change(view, :start_search, %{
        "search" => %{@default_search_params | "querystring" => "somestring"}
      })

      # wait for async search task to complete
      # TODO: find better way to test searching
      :timer.sleep(800)

      html = view |> element("#logs-list-container") |> render()

      assert html =~ "some correct message"
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

    setup [:setup_user_session, :setup_source_processes]

    test "on source with suggestion fields, creates flash with link to force query", %{
      conn: conn,
      source: source
    } do
      {:ok, view, _html} = live(conn, Routes.live_path(conn, SearchLV, source.id))

      :timer.sleep(800)

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

      :timer.sleep(800)

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

      :timer.sleep(800)

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
