defmodule LogflareWeb.MarketingControllerTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  setup do
    start_supervised!(Logflare.SystemMetricsSup)
    :ok
  end

  for action <- [
        :index,
        :contact,
        :pricing
      ] do
    test "public marketing path #{action} ", %{
      conn: conn
    } do
      if unquote(action) == :pricing do
        insert(:plan, price: 123, period: "month", name: "Metered")
        insert(:plan, price: 123, period: "month", name: "Metered BYOB")
      end

      path = Routes.marketing_path(conn, unquote(action))
      conn = conn |> get(path)
      assert conn.status == 200

      # has announcement banner
      assert html_response(conn, 200) =~ "now part of Supabase"
    end
  end

  for action <- [
        :terms,
        :privacy
      ] do
    test "legal redirect #{action}", %{
      conn: conn
    } do
      path = Routes.marketing_path(conn, unquote(action))
      conn = conn |> get(path)
      assert redirected_to(conn, 301) =~ "supabase.com"
    end
  end

  # redirect to docs site

  for action <- [
        :overview,
        :vercel_setup,
        :big_query_setup,
        :slack_app_setup,
        :data_studio_setup,
        :event_analytics_demo,
        :guides
      ] do
    test "docs site redirect #{action} ", %{
      conn: conn
    } do
      path = Routes.marketing_path(conn, unquote(action))
      conn = conn |> get(path)
      assert redirected_to(conn, 301) =~ "docs.logflare.app"
    end
  end

  describe "single tenant ui" do
    TestUtils.setup_single_tenant(seed_user: true)

    test "redirect to dashboard", %{conn: conn} do
      path = Routes.marketing_path(conn, :index)
      conn = get(conn, path)
      assert redirected_to(conn, 302) =~ "/dashboard"
    end
  end
end
