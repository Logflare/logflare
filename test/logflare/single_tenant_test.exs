defmodule Logflare.SingleTenantTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.SingleTenant
  alias Logflare.Billing
  alias Logflare.Users
  alias Logflare.User

  describe "single tenant mode" do
    TestUtils.setup_single_tenant()

    test "create_default_plan/0 creates default enterprise plan if not present" do
      assert {:ok, plan} = SingleTenant.create_default_plan()
      assert plan.name == "Enterprise"
      assert {:error, :already_created} = SingleTenant.create_default_plan()
    end

    test "get_default_user/0" do
      assert {:ok, _plan} = SingleTenant.create_default_plan()
      assert {:ok, _user} = SingleTenant.create_default_user()
      assert %User{name: "default"} = SingleTenant.get_default_user()
    end

    test "create_default_user/0, get_default_user/0 :inserts a default enterprise user if not present" do
      assert {:ok, _plan} = SingleTenant.create_default_plan()
      assert {:ok, user} = SingleTenant.create_default_user()
      plan = Billing.get_plan_by_user(user)
      assert plan.name == "Enterprise"
      assert {:error, :already_created} = SingleTenant.create_default_user()
    end

    test "single_tenant? returns true when in single tenant mode" do
      assert SingleTenant.single_tenant?()
    end

    test "Logflare.Application.startup_tasks/0 should insert plan and user" do
      Logflare.Application.startup_tasks()

      assert [_] = Billing.list_plans()
      assert 1 = Users.count_users()
    end
  end

  test "single_tenant? returns false when not in single tenant mode" do
    refute SingleTenant.single_tenant?()
  end
end
