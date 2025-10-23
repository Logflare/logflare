defmodule LogflareWeb.ExampleTest do
  use LogflareWeb.FeatureCase,
    async: true,
    # Show browser and pause 1 second between every interaction
    headless: false,
    slow_mo: :timer.seconds(1)

  test "open first page" do
    conn
    |> visit(~p"/")
    |> screenshot("home.png", full_page: true)
  end
end
