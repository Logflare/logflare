defmodule LogflareWeb.MarketingControllerTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  for action <- [
        :index,
        :contact,
        :pricing,
        :overview,
        :vercel_setup,
        :big_query_setup,
        :slack_app_setup,
        :data_studio_setup,
        :event_analytics_demo,
        :terms,
        :privacy,
        :cookies,
        :guides
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
    end
  end
end
