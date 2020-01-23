defmodule LogflareWeb.Source.SearchLVTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  import Phoenix.LiveViewTest
  alias Logflare.Sources
  alias Logflare.Users
  @endpoint LogflareWeb.Endpoint
  import Logflare.Factory
  use Placebo

  describe "form_update" do
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

      assert find_search_form_value(html, "#search_chart_period option[selected]") == "minute"
      assert find_search_form_value(html, "#search_chart_aggregate option") == "N/A"
      assert find_search_form_value(html, "#search_querystring") == ""
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
      # FIXME?
      # assert find_search_form_value(html, ".tailing_checkbox") == "false"

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
            "querystring" => "error crash chart:metadata.event_count",
            "chart_period" => "day",
            "chart_aggregate" => "sum",
            "tailing?" => "false"
          }
        })

      assert find_search_form_value(html, "#search_chart_period option[selected]") == "day"
      assert find_search_form_value(html, "#search_chart_aggregate option[selected]") == "sum"
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
    setup do
      user = insert(:user)
      user = Users.get(user.id)

      source = insert(:source, user: user)

      source = Sources.get(source.id)

      %{source: [source], user: [user]}
    end

    test "mount", %{conn: conn, source: [s | _], user: [u | _]} do
      conn =
        conn
        |> assign(:user, u)
        |> get("/sources/#{s.id}/search")

      assert html_response(conn, 200) =~ "source-logs-search-container"

      assert {:ok, view, html} = live(conn)
    end

    test "redirected mount", %{conn: conn, source: [s | _], user: [u | _]} do
      assert {:error, %{redirect: %{to: "/"}}} = live(conn, "/sources/1/search")
    end
  end

  describe "other functions" do
    setup do
      user = insert(:user)
      user = Users.get(user.id)

      source = insert(:source, user: user)

      source = Sources.get(source.id)

      %{source: [source], user: [user]}
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

      refute render_click(view, "remove_flash", %{"flash_key" => "warning"}) =~
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

  defp find_search_form_value(html, selector) do
    {:ok, document} = Floki.parse_document(html)

    document
    |> Floki.find(selector)
    |> Floki.attribute("value")
    |> hd
  end
end
