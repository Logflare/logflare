defmodule LogflareWeb.Source.SearchLVTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  import Phoenix.LiveViewTest
  alias Logflare.Sources
  alias Logflare.Users
  @endpoint LogflareWeb.Endpoint
  import Logflare.Factory
  use Placebo
  alias Logflare.BigQuery.PredefinedTestUser
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.Lql.ChartRule
  alias Logflare.Lql
  alias Logflare.Source
  @test_token :"2e051ba4-50ab-4d2a-b048-0dc595bfd6cf"

  setup_all do
    Logflare.Sources.Counters.start_link()
    :ok
  end

  describe "user action flow simulation" do
    setup [:assign_user_source]

    test "user sequence", %{conn: conn, source: [s | _]} do
      {:ok, view, html} = live(conn, "/sources/#{s.id}/search")

      assert find_search_form_value(html, "#search_chart_period option[selected]") == "minute"
      assert find_search_form_value(html, "#search_chart_aggregate option") == "count"

      assert find_search_form_value(html, "#search_querystring") ==
               "c:count(*) c:group_by(t::minute)"

      # assert find_search_form_value(html, ".tailing_checkbox") == "true"

      html =
        render_change(view, :form_update, %{
          "search" => %{
            "querystring" => "c:count(*) c:group_by(t::minute)",
            "chart_period" => "minute",
            "chart_aggregate" => "count",
            "tailing?" => "false"
          }
        })

      assert find_search_form_value(html, "#search_chart_period option[selected]") == "minute"
      assert find_search_form_value(html, "#search_chart_aggregate option") == "count"

      assert find_search_form_value(html, "#search_querystring") ==
               "c:count(*) c:group_by(t::minute)"

      html =
        render_change(view, :form_update, %{
          "search" => %{
            "querystring" => "c:count(*) c:group_by(t::minute) error crash",
            "chart_period" => "minute",
            "chart_aggregate" => "count",
            "tailing?" => "false"
          }
        })

      lql_rules = get_view_assigns(view).lql_rules
      assert Lql.Utils.get_chart_rule(lql_rules).aggregate == :count
      assert find_search_form_value(html, "#search_chart_period option[selected]") == "minute"

      assert find_search_form_value(html, "#search_querystring") ==
               "c:count(*) c:group_by(t::minute) error crash"

      html =
        render_change(view, :form_update, %{
          "search" => %{
            "querystring" => "c:count(*) c:group_by(t::minute) error crash",
            "chart_period" => "day",
            "chart_aggregate" => "count",
            "tailing?" => "false"
          }
        })

      lql_rules = get_view_assigns(view).lql_rules
      assert Lql.Utils.get_chart_rule(lql_rules).aggregate == :count

      assert find_search_form_value(html, "#search_querystring") ==
               "c:count(*) c:group_by(t::day)"

      assert find_search_form_value(html, "#search_chart_period option[selected]") == "day"
      assert find_search_form_value(html, "#search_chart_aggregate option[selected]") == "count"

      html =
        render_change(view, :start_search, %{
          "search" => %{
            "querystring" => "c:count(*) c:group_by(t::day) error crash"
          }
        })

      assert find_search_form_value(html, "#search_querystring") ==
               "error crash c:count(*) c:group_by(t::day)"

      lql_rules = get_view_assigns(view).lql_rules
      chart_rule = Lql.Utils.get_chart_rule(lql_rules)
      assert chart_rule.aggregate == :count
      assert chart_rule.period == :day
      assert find_search_form_value(html, "#search_chart_period option[selected]") == "day"
      assert find_search_form_value(html, "#search_chart_aggregate option[selected]") == "count"

      html =
        render_change(view, :start_search, %{
          "search" => %{
            "querystring" => "c:sum(m.int_field_1) c:group_by(t::minute) error crash"
          }
        })

      assert find_search_form_value(html, "#search_querystring") ==
               "error crash c:sum(m.int_field_1) c:group_by(t::minute)"

      lql_rules = get_view_assigns(view).lql_rules
      chart_rule = Lql.Utils.get_chart_rule(lql_rules)
      assert chart_rule.aggregate == :sum
      assert chart_rule.period == :minute
      assert find_search_form_value(html, "#search_chart_period option[selected]") == "minute"
      assert find_search_form_value(html, "#search_chart_aggregate option[selected]") == "sum"

      assert get_view_assigns(view).tailing? == true

      html = render_change(view, :stop_live_search, %{})
      assert get_view_assigns(view).tailing? == false

      html =
        render_change(view, :datepicker_update, %{
          "querystring" => "t:2020-01-01T01:10:00..2020-02-01T10:22:20"
        })

      lql_rules = get_view_assigns(view).lql_rules

      assert Lql.Utils.get_ts_filters(lql_rules) == [
               %Logflare.Lql.FilterRule{
                 modifiers: %{},
                 operator: :range,
                 path: "timestamp",
                 shorthand: nil,
                 value: nil,
                 values: [~N[2020-01-01 01:10:00], ~N[2020-02-01 10:22:20]]
               }
             ]

      assert find_search_form_value(html, "#search_querystring") ==
               "error crash t:2020-01-01T01:10:00..2020-02-01T10:22:20 c:sum(m.int_field_1) c:group_by(t::minute)"

      lql_rules = get_view_assigns(view).lql_rules
      chart_rule = Lql.Utils.get_chart_rule(lql_rules)
      assert chart_rule.aggregate == :sum
      assert chart_rule.period == :minute
      assert find_search_form_value(html, "#search_chart_period option[selected]") == "minute"
      assert find_search_form_value(html, "#search_chart_aggregate option[selected]") == "sum"

      html =
        render_change(view, :start_search, %{
          "search" => %{
            "querystring" =>
              "error crash t:2020-01-01T01:10:00..2020-02-01T10:22:20 c:avg(m.int_field_1) c:group_by(t::hour)"
          }
        })

      lql_rules = get_view_assigns(view).lql_rules
      chart_rule = Lql.Utils.get_chart_rule(lql_rules)
      assert chart_rule.aggregate == :avg
      assert chart_rule.period == :hour
      assert find_search_form_value(html, "#search_chart_period option[selected]") == "hour"
      assert find_search_form_value(html, "#search_chart_aggregate option[selected]") == "avg"

      html =
        render_change(view, :start_search, %{
          "search" => %{
            "querystring" =>
              "error crash t:2020-01-01T01:10:00..2020-02-01T10:22:20 c:avg(m.int_field_1) c:group_by(t::hour)"
          }
        })

      lql_rules = get_view_assigns(view).lql_rules
      chart_rule = Lql.Utils.get_chart_rule(lql_rules)
      assert chart_rule.aggregate == :avg
      assert chart_rule.period == :hour
      assert find_search_form_value(html, "#search_chart_period option[selected]") == "hour"
      assert find_search_form_value(html, "#search_chart_aggregate option[selected]") == "avg"
    end
  end

  describe "form_update" do
    setup [:assign_user_source]

    test "generates correct querystring", %{conn: conn, source: [s | _]} do
      {:ok, view, html} = live(conn, "/sources/#{s.id}/search")

      html =
        render_change(view, :form_update, %{
          "search" => %{
            "querystring" => ""
          }
        })

      assert get_view_assigns(view).querystring == ~s|c:count(*) c:group_by(t::minute)|
      assert html =~ ~s|c:count(*) c:group_by(t::minute)|
    end
  end

  describe "mount" do
    setup [:assign_user_source]

    test "successfull for source owner", %{conn: conn, source: [s | _], user: [u | _]} do
      conn =
        conn
        |> assign(:user, u)
        |> live("/sources/#{s.id}/search")

      assert {:ok, view, html} = conn

      assert get_view_assigns(view).lql_rules == [
               %ChartRule{
                 aggregate: :count,
                 path: "timestamp",
                 period: :minute,
                 value_type: :datetime
               }
             ]
    end

    test "shows notification error for malformed query", %{
      conn: conn,
      source: [s | _],
      user: [u | _]
    } do
      conn = live(conn, "/sources/#{s.id}/search?q=t:20020")

      assert {:ok, view, html} = conn

      assert get_view_assigns(view).notifications == %{
               error:
                 "Error while parsing timestamp filter value: expected ISO8601 string or range or shorthand, got '20020'"
             }

      assert get_view_assigns(view).lql_rules == []
    end

    test "redirected for non-owner user", %{conn: conn, source: [s | _], user: [u | _]} do
      u = %{u | id: u.id - 1}
      conn = assign(conn, :user, u)
      assert {:error, %{redirect: %{to: "/"}}} = live(conn, "/sources/#{s.id}/search")
    end

    test "redirected for anonymous user", %{conn: conn, source: [s | _], user: [u | _]} do
      conn = assign(conn, :user, nil)
      assert {:error, %{redirect: %{to: "/"}}} = live(conn, "/sources/#{s.id}/search")
    end
  end

  describe "other functions" do
    setup [:assign_user_source]

    test "stop/start live search", %{conn: conn, source: [s]} do
      conn =
        conn
        |> live("/sources/#{s.id}/search?q=error")

      {:ok, view, html} = conn

      assert get_view_assigns(view).tailing?

      render_click(view, "stop_live_search", %{})

      refute get_view_assigns(view).tailing?

      render_click(view, "start_live_search", %{})

      assert get_view_assigns(view).tailing?
    end

    test "datepicker_update", %{conn: conn, source: [s | _], user: [u | _]} do
      conn =
        conn
        |> assign(:user, u)
        |> live("/sources/#{s.id}/search?q=error")

      {:ok, view, html} = conn

      assert get_view_assigns(view).querystring == "error c:count(*) c:group_by(t::minute)"

      assert html =~ "error c:count(*) c:group_by(t::minute)"

      assert render_change(view, "datepicker_update", %{"querystring" => "t:last@2h"}) =~
               ~S|id="user-local-timezone"|

      assert "error t:last@2hour c:count(*) c:group_by(t::minute)" ==
               get_view_assigns(view).querystring
    end

    test "set_local_time", %{conn: conn, source: [s | _], user: [u | _]} do
      conn =
        conn
        |> assign(:user, u)
        |> get("/sources/#{s.id}/search")

      assert html_response(conn, 200) =~ "source-logs-search-container"
      {:ok, view, html} = live(conn)

      assert render_click(view, "set_local_time", %{"use_local_time" => "true"}) =~
               ~S|id="user-local-timezone"|
    end

    test "user_idle", %{conn: conn, source: [s | _], user: [u | _]} do
      conn =
        conn
        |> assign(:user, u)
        |> get("/sources/#{s.id}/search")

      assert html_response(conn, 200) =~ "source-logs-search-container"
      {:ok, view, html} = live(conn)

      assert render_click(view, "user_idle", %{}) =~
               "Live search paused due to user inactivity."

      refute render_click(view, "remove_notifications", %{"notifications_key" => "warning"}) =~
               "Live search paused due to user inactivity."
    end

    test "activate_modal/deactivate_modal", %{conn: conn, source: [s | _], user: [u | _]} do
      conn =
        conn
        |> assign(:user, u)
        |> get("/sources/#{s.id}/search")

      assert html_response(conn, 200) =~ "source-logs-search-container"
      {:ok, view, html} = live(conn)

      assert render_click(view, "activate_modal", %{"modal_id" => "searchHelpModal"}) =~
               "Search Your Log Events"

      refute render_click(view, "deactivate_modal", %{}) =~
               "Search Your Log Events"
    end
  end

  defp assign_user_source(context) do
    {:ok, _} = RLS.start_link(%RLS{source_id: @test_token})
    Process.sleep(2500)
    user = Users.get_by_and_preload(email: System.get_env("LOGFLARE_TEST_USER_WITH_SET_IAM"))

    Sources.Cache.put_bq_schema(@test_token, PredefinedTestUser.table_schema())
    source = Sources.get_by(token: @test_token)

    conn =
      build_conn
      |> assign(:user, user)

    %{source: [source], user: [user], conn: conn}
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
end
