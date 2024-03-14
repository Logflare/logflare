defmodule LogflareWeb.BillingAccountLiveTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  alias Logflare.Sources.Counters
  alias Logflare.Sources.RateCounters
  alias Logflare.SystemMetrics.AllLogsLogged

  setup %{conn: conn} do
    start_supervised!(AllLogsLogged)
    start_supervised!(Counters)
    start_supervised!(RateCounters)

    insert(:plan, name: "Free")
    insert(:plan, name: "Metered")
    insert(:plan, name: "Metered BYOB")
    user = insert(:user)

    conn =
      conn
      |> login_user(user)

    Stripe.Customer
    |> expect(:create, 1, fn _user -> {:ok, %{id: TestUtils.random_string()}} end)

    conn |> post(Routes.billing_path(conn, :create))
    {:ok, user: user, conn: conn}
  end

  test "can view billing page", %{conn: conn} do
    {:ok, view, _html} = live(conn, "/billing/edit")
    # static
    html = render(view)
    assert html =~ "~/billing/edit"
    assert has_element?(view, "h1,h2,h3,h4", "Billing Account")
    assert has_element?(view, "p", "You're currently on the Free plan")
  end
end
