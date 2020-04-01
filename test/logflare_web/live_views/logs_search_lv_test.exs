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
  alias Logflare.Lql.ChartRule
  @test_token :"2e051ba4-50ab-4d2a-b048-0dc595bfd6cf"

  describe "form_update" do
    setup [:assign_user_source]

    test "with valid search params", %{conn: conn} do
      assert html_response(conn, 200) =~ "source-logs-search-container"
      assert {:ok, view, html} = live(conn)

      assert find_search_form_value(html, "#search_chart_period option[selected]") == "minute"
      assert find_search_form_value(html, "#search_chart_aggregate option") == "N/A"

      assert find_search_form_value(html, "#search_querystring") ==
               "c:count(*) c:group_by(t::minute)"

      assert find_search_form_value(html, ".tailing_checkbox") == "true"

      html =
        render_change(view, :form_update, %{
          "search" => %{
            "querystring" => "error crash",
            "chart_period" => "second",
            "chart_aggregate" => "count",
            "tailing?" => "false"
          }
        })

      assert find_search_form_value(html, "#search_chart_period option[selected]") == "second"
      assert find_search_form_value(html, "#search_chart_aggregate option") == "N/A"
      assert find_search_form_value(html, "#search_querystring") == "error crash"

      html =
        render_change(view, :form_update, %{
          "search" => %{
            "querystring" => "error crash",
            "chart_period" => "hour",
            "chart_aggregate" => "count",
            "tailing?" => "false"
          }
        })

      assert find_search_form_value(html, "#search_chart_period option[selected]") == "hour"

      html =
        render_change(view, :form_update, %{
          "search" => %{
            "querystring" => "error crash c:count(metadata.event_count)",
            "chart_period" => "day",
            "chart_aggregate" => "sum",
            "tailing?" => "false"
          }
        })

      assert find_search_form_value(html, "#search_chart_period option[selected]") == "day"
      assert find_search_form_value(html, "#search_chart_aggregate option[selected]") == "sum"
    end

    test "generates correct querystring", %{conn: conn} do
      assert html_response(conn, 200) =~ "source-logs-search-container"
      assert {:ok, view, html} = live(conn)

      html =
        render_change(view, :form_update, %{
          "search" => %{
            "querystring" => ~s||,
            "chart_period" => "minute",
            "chart_aggregate" => "count",
            "tailing?" => "false"
          }
        })

      assert :sys.get_state(view.pid).socket.assigns.querystring ==
               ~s|c:count(*) c:group_by(t::minute)|

      assert html =~ ~s|c:count(*) c:group_by(t::minute)|
    end
  end

  describe "form_submit" do
    setup do
      user = insert(:user)
      user = Users.get(user.id)

      source = insert(:source, user: user)

      source = Sources.get(source.id)

      conn =
        build_conn
        |> assign(:user, user)
        |> get("/sources/#{source.id}/search")

      %{source: [source], user: [user], conn: conn}
    end

    test "with valid search params", %{conn: conn} do
      assert html_response(conn, 200) =~ "source-logs-search-container"
      assert {:ok, view, html} = live(conn)

      html =
        render_change(view, :form_update, %{
          "search" => %{
            "querystring" => "error crash chart:metadata.event_count",
            "chart_period" => "hour",
            "chart_aggregate" => "avg",
            "tailing?" => "false"
          }
        })

      assert find_search_form_value(html, "#search_chart_period option[selected]") == "hour"
      assert find_search_form_value(html, "#search_chart_aggregate option[selected]") == "avg"

      html =
        render_submit(view, :start_search, %{
          "search" => %{}
        })

      {:ok, document} = Floki.parse_document(html)

      assert document
             |> Floki.find("button#search")
             |> Floki.attribute("disabled") == [""]
    end
  end

  describe "mount" do
    setup [:assign_user_source]

    test "successfull for source owner", %{conn: conn, source: [s | _], user: [u | _]} do
      conn =
        conn
        |> assign(:user, u)
        |> get("/sources/#{s.id}/search")

      assert html_response(conn, 200) =~ "source-logs-search-container"

      assert {:ok, view, html} = live(conn)

      assert :sys.get_state(view.pid).socket.assigns.lql_rules == [
               %ChartRule{
                 aggregate: :count,
                 path: "timestamp",
                 period: :minute,
                 value_type: nil
               }
             ]
    end

    test "redirected for non-owner user", %{conn: conn, source: [s | _], user: [u | _]} do
      u = %{u | id: u.id - 1}
      assert {:error, %{redirect: %{to: "/"}}} = live(conn, "/sources/1/search")
    end

    test "redirected for anonymous user", %{conn: conn, source: [s | _], user: [u | _]} do
      assert {:error, %{redirect: %{to: "/"}}} = live(conn, "/sources/1/search")
    end
  end

  describe "other functions" do
    setup [:assign_user_source]

    @tag :run
    test "datepicker_update", %{conn: conn, source: [s | _], user: [u | _]} do
      conn =
        conn
        |> assign(:user, u)
        |> get("/sources/#{s.id}/search?q=error")

      assert html_response(conn, 200) =~ "c:count(*) c:group_by(t::minute) error"
      {:ok, view, html} = live(conn)

      assert :sys.get_state(view.pid).socket.assigns.querystring ==
               "c:count(*) c:group_by(t::minute) error"

      assert render_change(view, "datepicker_update", %{"querystring" => "t:last@2h"}) =~
               ~S|id="user-local-timezone"|

      assert "c:count(*) c:group_by(t::minute) error t:last@2hour" =~
               :sys.get_state(view.pid).socket.assigns.querystring
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
    user = Users.get_by_and_preload(email: System.get_env("LOGFLARE_TEST_USER_WITH_SET_IAM"))
    Sources.Cache.put_bq_schema(@test_token, PredefinedTestUser.table_schema())
    source = Sources.get_by(token: @test_token)

    conn =
      build_conn
      |> assign(:user, user)
      |> get("/sources/#{source.id}/search")

    %{source: [source], user: [user], conn: conn}
  end

  defp find_search_form_value(html, selector) do
    {:ok, document} = Floki.parse_document(html)

    document
    |> Floki.find(selector)
    |> Floki.attribute("value")
    |> hd
  end
end
