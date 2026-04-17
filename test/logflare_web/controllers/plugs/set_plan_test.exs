defmodule LogflareWeb.Plugs.SetPlanTest do
  use LogflareWeb.ConnCase, async: true

  alias Logflare.Billing
  alias LogflareWeb.Plugs.SetPlan

  describe "init/1" do
    test "returns nil" do
      assert SetPlan.init([]) == nil
    end
  end

  describe "call/2" do
    test "assigns plan when user is present", %{conn: conn} do
      plan = insert(:plan)
      user = insert(:user)

      result =
        conn
        |> assign(:user, user)
        |> SetPlan.call([])

      assert result.assigns.plan.id == plan.id
    end

    test "assigns legacy plan when billing is disabled", %{conn: conn} do
      insert(:plan)
      user = insert(:user, billing_enabled: false)

      result =
        conn
        |> assign(:user, user)
        |> SetPlan.call([])

      assert result.assigns.plan == Billing.legacy_plan()
    end

    test "passes conn through when no user is assigned", %{conn: conn} do
      result = SetPlan.call(conn, [])

      refute Map.has_key?(result.assigns, :plan)
    end

    test "passes conn through when user assign is nil", %{conn: conn} do
      result =
        conn
        |> assign(:user, nil)
        |> SetPlan.call([])

      refute Map.has_key?(result.assigns, :plan)
    end

    test "uses default opts when none provided", %{conn: conn} do
      plan = insert(:plan)
      user = insert(:user)

      result =
        conn
        |> assign(:user, user)
        |> SetPlan.call()

      assert result.assigns.plan.id == plan.id
    end
  end

  describe "browser pipeline integration" do
    test "plan is assigned for authenticated browser requests" do
      plan = insert(:plan)
      user = insert(:user)
      _team = insert(:team, user: user)

      conn =
        build_conn()
        |> login_user(user)
        |> get("/guides")

      assert conn.assigns.plan.id == plan.id
    end

    test "plan is not assigned for unauthenticated browser requests" do
      insert(:plan)

      conn =
        build_conn()
        |> get("/guides")

      refute Map.has_key?(conn.assigns, :plan)
    end
  end
end
