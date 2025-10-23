defmodule LogflareWeb.ExampleTest do
  use LogflareWeb.FeatureCase,
    headless: true,
    slow_mo: :timer.seconds(40)

  TestUtils.setup_single_tenant(seed_user: true)

  test "open first page", %{conn: conn} do
    # insert(:plan, price: 123, period: "month", name: "Metered") |> dbg()
    # insert(:plan, price: 123, period: "month", name: "Metered BYOB")

    conn
    |> visit(~p"/dashboard")
    |> screenshot("home.png", full_page: true)
  end
end
