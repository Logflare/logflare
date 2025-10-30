defmodule LogflareWeb.AdminPlanControllerTest do
  use LogflareWeb.ConnCase, async: true

  alias Logflare.Billing

  setup do
    insert(:plan)

    :ok
  end

  describe "Listing Plans" do
    setup do
      admin = insert(:user, admin: true)
      {:ok, admin: admin}
    end

    test "displays list of plans for admin users", %{conn: conn, admin: admin} do
      _plan1 = insert(:plan, name: "Free Plan", price: 0, period: "month")
      _plan2 = insert(:plan, name: "Pro Plan", price: 2000, period: "month")

      conn
      |> login_user(admin)
      |> visit(~p"/admin/plans")
      |> assert_has("h5", text: "~/admin/plans")
      |> assert_has("h1", text: "Plans")
      |> assert_has("table")
      |> assert_has("tbody tr:first-child td:nth-child(2)", text: "Free")
      |> assert_has("tbody tr:nth-child(2) td:nth-child(2)", text: "Free Plan")
      |> assert_has("tbody tr:nth-child(3) td:nth-child(2)", text: "Pro Plan")
      |> assert_has("td", text: "0")
      |> assert_has("td", text: "2000")
      |> assert_has("a", text: "New plan")
      |> assert_has("a", text: "Edit")
    end
  end

  describe "New Plan" do
    setup do
      admin = insert(:user, admin: true)

      {:ok, admin: admin}
    end

    test "creates new plan", %{conn: conn, admin: admin} do
      assert conn
             |> login_user(admin)
             |> visit(~p"/admin/plans")
             |> click_link("New plan")
             |> assert_path(~p"/admin/plans/new")
             |> assert_has("h5", text: "~/admin/plans/new")
             |> assert_has("h1", text: "New Plan")
             |> assert_has("button[type='submit']")
             |> fill_in("input[name='plan[name]']", "Name", with: "Test Plan")
             |> fill_in("input[name='plan[stripe_id]']", "Stripe", with: "price_test123")
             |> fill_in("input[name='plan[period]']", "Period", with: "month")
             |> fill_in("input[name='plan[price]']", "Price", with: "1500")
             |> fill_in("input[name='plan[limit_rate_limit]']", "Account Rate Limit (per second)",
               with: "100"
             )
             |> fill_in(
               "input[name='plan[limit_source_rate_limit]']",
               "Source Rate Limit (per second)",
               with: "50"
             )
             |> fill_in("input[name='plan[limit_alert_freq]']", "Alerts Every (milliseconds)",
               with: "60000"
             )
             |> fill_in(
               "input[name='plan[limit_saved_search_limit]']",
               "Saved Searches (per source)",
               with: "5"
             )
             |> fill_in("input[name='plan[limit_team_users_limit]']", "Team Users", with: "3")
             |> fill_in("input[name='plan[limit_source_fields_limit]']", "Source Fields",
               with: "300"
             )
             |> fill_in("input[name='plan[limit_source_ttl]']", "Source TTL", with: "259200000")
             |> submit()
             |> assert_path(~p"/admin/plans")
             |> assert_has("#flash-info", text: "Plan created!")
             |> assert_has("tbody tr:first-child td:nth-child(2)", text: "Free")
             |> assert_has("tbody tr:nth-child(2) td:nth-child(2)", text: "Test Plan")

      assert plan = Billing.get_plan_by(name: "Test Plan")
      assert plan.name == "Test Plan"
      assert plan.stripe_id == "price_test123"
      assert plan.price == 1500
      assert plan.period == "month"
    end

    test "creates plan with default values when minimal params provided", %{
      conn: conn,
      admin: admin
    } do
      assert conn
             |> login_user(admin)
             |> visit(~p"/admin/plans/new")
             |> assert_has("h1", text: "New Plan")
             |> assert_has("button[type='submit']")
             |> fill_in("input[name='plan[name]']", "Name", with: "Minimal Plan")
             |> submit()
             |> assert_path(~p"/admin/plans")
             |> assert_has("#flash-info", text: "Plan created!")
             |> assert_has("tbody tr:first-child td:nth-child(2)", text: "Free")
             |> assert_has("tbody tr:nth-child(2) td:nth-child(2)", text: "Minimal Plan")

      assert plan = Billing.get_plan_by(name: "Minimal Plan")
      assert plan.name == "Minimal Plan"
      assert plan.period == "month"
      assert plan.price == 0
    end
  end

  describe "Edit Plan" do
    setup do
      admin = insert(:user, admin: true)

      {:ok, admin: admin}
    end

    test "update existing plan in edit form", %{conn: conn, admin: admin} do
      plan =
        insert(:plan,
          name: "Existing Plan",
          stripe_id: "price_existing",
          price: 1000,
          period: "month",
          limit_sources: 25
        )

      conn
      |> login_user(admin)
      |> visit(~p"/admin/plans")
      |> click_link("a[href='#{~p"/admin/plans/#{plan}/edit"}']", "Edit")
      |> assert_path(~p"/admin/plans/#{plan}/edit")
      |> assert_has("h1", text: "Edit Plan")
      |> assert_has("form")
      |> assert_has("input[name='plan[name]'][value='Existing Plan']")
      |> assert_has("input[name='plan[stripe_id]'][value='price_existing']")
      |> assert_has("input[name='plan[price]'][value='1000']")
      |> assert_has("input[name='plan[period]'][value='month']")
      |> assert_has("input[name='plan[limit_sources]'][value='25']")
      |> assert_has("button[type='submit']")
      |> fill_in("input[name='plan[name]']", "Name", with: "Updated Plan")
      |> fill_in("input[name='plan[price]']", "Price", with: "1500")
      |> submit()

      assert updated_plan = Billing.get_plan!(plan.id)
      assert updated_plan.name == "Updated Plan"
      assert updated_plan.price == 1500
    end

    test "raises error for non-existent plan", %{conn: conn, admin: admin} do
      assert_raise Ecto.NoResultsError, fn ->
        conn
        |> login_user(admin)
        |> get(~p"/admin/plans/999999/edit")
      end
    end

    test "returns 403 for unauthenticated requests and for non-admin users", %{conn: conn} do
      user = insert(:user, admin: false)
      plan = insert(:plan, name: "Test Plan for Edit")

      assert conn
             |> login_user(user)
             |> get(~p"/admin/plans/#{plan}/edit")
             |> html_response(403) =~ "Forbidden"

      assert conn
             |> put(~p"/admin/plans/#{plan}", %{plan: %{id: plan.id, name: "Updated"}})
             |> html_response(403) =~ "Forbidden"

      assert conn
             |> login_user(user)
             |> put(~p"/admin/plans/#{plan}", %{plan: %{id: plan.id, name: "Updated"}})
             |> html_response(403) =~ "Forbidden"
    end
  end

  test "returns 403 for unauthenticated requests and for non-admin users", %{conn: conn} do
    user = insert(:user)

    for path <- [~p"/admin/plans", ~p"/admin/plans/new"] do
      assert conn
             |> login_user(user)
             |> get(path)
             |> html_response(403) =~ "Forbidden"

      assert conn
             |> get(path)
             |> html_response(403) =~ "Forbidden"
    end
  end
end
