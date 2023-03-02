defmodule LogflareWeb.Source.SearchLVTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  @endpoint LogflareWeb.Endpoint
  import Phoenix.LiveViewTest
  alias Logflare.Sources.Counters
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.Source.BigQuery.Schema
  alias Logflare.Logs.SearchQueryExecutor
  alias LogflareWeb.Source.SearchLV
  alias Logflare.SingleTenant

  @default_search_params %{
    "querystring" => "c:count(*) c:group_by(t::minute)",
    "chart_period" => "minute",
    "chart_aggregate" => "count",
    "tailing?" => "false"
  }

  defp setup_mocks(_ctx) do
    # mocks
    Goth
    |> stub(:fetch, fn _mod -> {:ok, %Goth.Token{token: "auth-token"}} end)

    GoogleApi.BigQuery.V2.Api.Jobs
    |> stub(:bigquery_jobs_query, fn _conn, _proj_id, _opts ->
      {:ok, TestUtils.gen_bq_response()}
    end)

    :ok
  end

  defp on_exit_kill_tasks(_ctx) do
    on_exit(fn ->
      Logflare.Utils.Tasks.kill_all_tasks()
    end)

    :ok
  end

  # requires a user, source, and plan set
  defp setup_source_processes(%{source: source, plan: plan}) do
    start_supervised!(Counters)
    rls = %RLS{source_id: source.token, plan: plan}
    start_supervised!({Schema, rls})
    start_supervised!({SearchQueryExecutor, rls})
    :ok
  end

  # to simulate signed in user.
  defp setup_user_session(%{conn: conn, user: user, plan: plan}) do
    _billing_account = insert(:billing_account, user: user, stripe_plan_id: plan.stripe_id)
    user = user |> Logflare.Repo.preload(:billing_account)
    conn = conn |> put_session(:user_id, user.id) |> assign(:user, user)
    [conn: conn]
  end

  # do this for all tests
  setup [:setup_mocks, :on_exit_kill_tasks]

  describe "search tasks" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)
      plan = insert(:plan)
      [user: user, source: source, plan: plan]
    end

    setup [:setup_user_session, :setup_source_processes]

    test "load page", %{conn: conn, source: source} do
      {:ok, view, html} = live(conn, Routes.live_path(conn, SearchLV, source.id))

      assert html =~ "~/logs/"
      assert html =~ source.name
      assert html =~ "/search"

      # wait for async search task to complete
      :timer.sleep(1000)
      html = view |> element("#logs-list-container") |> render()
      assert html =~ "some event message"

      html = view |> render()
      assert html =~ "Elapsed since last query"

      # default input values
      assert find_selected_chart_period(html) == "minute"
      assert find_chart_aggregate(html) == "count"
      assert find_querystring(html) == "c:count(*) c:group_by(t::minute)"
    end

    test "static elements", %{conn: conn, source: source} do
      {:ok, view, _html} = live(conn, Routes.live_path(conn, SearchLV, source.id))

      assert view
             |> element("a", "LQL")
             |> render_click() =~ "Event Message Filtering"
    end

    test "lql filters", %{conn: conn, source: source} do
      {:ok, view, _html} = live(conn, Routes.live_path(conn, SearchLV, source.id))

      :timer.sleep(1000)

      html = view |> element("#logs-list-container") |> render()
      assert html =~ "some event message"

      GoogleApi.BigQuery.V2.Api.Jobs
      |> stub(:bigquery_jobs_query, fn _conn, _proj_id, opts ->
        params = opts[:body].queryParameters

        if length(params) > 2 do
          assert Enum.any?(params, fn param -> param.parameterValue.value == "crasher" end)
          assert Enum.any?(params, fn param -> param.parameterValue.value == "error" end)
        end

        {:ok, TestUtils.gen_bq_response(%{"event_message" => "some error message"})}
      end)

      view
      |> render_change(:form_update, %{
        "search" => %{
          @default_search_params
          | "querystring" => "c:count(*) c:group_by(t::minute) error crasher"
        }
      })

      :timer.sleep(1000)

      view
      |> render_change(:start_search, %{
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
      schema =
        TestUtils.build_bq_schema(%{"metadata" => %{"nested" => "something"}, "top" => "level"})

      Schema.update(source.token, schema)
      # wait for schema to update
      # TODO: find a better way to test a source schema structure
      :timer.sleep(600)

      GoogleApi.BigQuery.V2.Api.Jobs
      |> stub(:bigquery_jobs_query, fn _conn, _proj_id, opts ->
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

      view
      |> render_change(:start_search, %{
        "search" => %{
          @default_search_params
          | "querystring" => "m.nested:test top:test"
        }
      })

      # wait for async search task to complete
      # TODO: find better way to test searching
      :timer.sleep(800)

      html = view |> element("#logs-list-container") |> render()

      assert html =~ "some correct message"
    end

    test "chart display interval", %{conn: conn, source: source} do
      GoogleApi.BigQuery.V2.Api.Jobs
      |> stub(:bigquery_jobs_query, fn _conn, _proj_id, opts ->
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

      view
      |> render_change(:start_search, %{
        "search" => %{
          @default_search_params
          | "chart_period" => "day"
        }
      })

      # wait for async search task to complete
      :timer.sleep(500)

      html = view |> element("#logs-list-container") |> render()

      assert html =~ "some event message"
    end

    test "shows flash error for malformed query", %{conn: conn, source: source} do
      assert {:ok, view, _html} =
               live(conn, Routes.live_path(conn, SearchLV, source, querystring: "t:20022"))

      assert view |> render() =~ "Error while parsing timestamp filter"
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

      assert get_flash(conn, "error") =~ "must be logged in"
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

    test "set_local_time", %{conn: conn, source: source} do
      {:ok, view, _html} = live(conn, Routes.live_path(conn, SearchLV, source))
      # post-init fetching
      :timer.sleep(500)

      assert render_click(view, "set_local_time", %{"use_local_time" => "true"}) =~
               ~S|id="user-local-timezone"|
    end

    test "datetime_update", %{conn: conn, source: source} do
      {:ok, view, _html} =
        live(conn, Routes.live_path(conn, SearchLV, source, querystring: "error"))

      # post-init fetching
      :timer.sleep(500)

      view
      |> render_change("datetime_update", %{"querystring" => "t:last@2h"})

      assert get_view_assigns(view).querystring =~ "t:last@2hour"
      assert get_view_assigns(view).querystring =~ "error"

      view
      |> render_change("datetime_update", %{
        "querystring" => "t:2020-04-20T00:{01..02}:00",
        "period" => "second"
      })

      assert get_view_assigns(view).querystring =~ "error"
      assert get_view_assigns(view).querystring =~ "t:2020-04-20T00:{01..02}:00"
    end
  end

  describe "single tenant searching" do
    TestUtils.setup_single_tenant(seed: true)

    setup do
      user = SingleTenant.get_default_user()
      source = insert(:source, user: user)
      plan = SingleTenant.get_default_plan()
      [user: user, source: source, plan: plan]
    end

    setup [:setup_user_session, :setup_source_processes]

    test "run a query", %{conn: conn, source: source} do
      GoogleApi.BigQuery.V2.Api.Jobs
      |> stub(:bigquery_jobs_query, fn _conn, _proj_id, opts ->
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

      view
      |> render_change(:start_search, %{
        "search" => %{
          @default_search_params
          | "querystring" => "somestring"
        }
      })

      # wait for async search task to complete
      # TODO: find better way to test searching
      :timer.sleep(800)

      html = view |> element("#logs-list-container") |> render()

      assert html =~ "some correct message"
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
