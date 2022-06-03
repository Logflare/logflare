defmodule Logflare.BillingTest do
  use Logflare.DataCase
  alias Logflare.{User, Billing, Billing.BillingAccount, Billing.PaymentMethod, Billing.Plan}

  describe "billing accounts" do
    @valid_attrs %{stripe_customer: "some stripe id"}
    @update_attrs %{stripe_customer: "some stripe other id"}
    test "list_billing_accounts/0" do
      ba = insert(:billing_account)
      id = ba.id
      assert [%BillingAccount{}] = Billing.list_billing_accounts()

      assert %BillingAccount{id: ^id} =
               Billing.get_billing_account_by(stripe_customer: ba.stripe_customer)

      assert %BillingAccount{id: ^id} = Billing.get_billing_account!(ba.id)
    end

    test "create_billing_account/2, update_billing_account/2, delete_billing_account/1" do
      user = insert(:user) |> Logflare.Repo.preload(:billing_account)

      assert {:ok, %BillingAccount{} = created} =
               Billing.create_billing_account(user, @valid_attrs)

      assert created.stripe_customer == @valid_attrs.stripe_customer

      assert {:ok, updated} = Billing.update_billing_account(created, @update_attrs)
      assert updated.stripe_customer == @update_attrs.stripe_customer

      user = Logflare.Repo.get!(User, user.id) |> Logflare.Repo.preload(:billing_account)
      assert {:ok, _} = Billing.delete_billing_account(user)

      assert_raise Ecto.StaleEntryError, fn ->
        Billing.delete_billing_account(user)
      end
    end

    test "stripe get in helpers" do
      ba = build(:billing_account)
      assert Billing.get_billing_account_stripe_plan(ba) != nil
      assert Billing.get_billing_account_stripe_subscription_item(ba) != nil
    end
  end

  describe "payment methods" do
    @valid_attrs %{price_id: "some price_id", stripe_id: "some stripe_id"}
    @update_attrs %{price_id: "some updated price_id", stripe_id: "some updated stripe_id"}
    @invalid_attrs %{price_id: nil, stripe_id: nil, customer_id: nil}
    setup do
      {:ok, billing_account: insert(:billing_account)}
    end

    test "list_payment_methods_by/1, get_payment_method!/1, get_payment_method_by/1 returns correctly",
         %{billing_account: ba} do
      payment_method = insert(:payment_method, customer_id: ba.stripe_customer)
      assert Billing.list_payment_methods_by(customer_id: ba.stripe_customer) == [payment_method]
      assert Billing.get_payment_method!(payment_method.id) == payment_method
      assert Billing.get_payment_method_by(customer_id: ba.stripe_customer) == payment_method
    end

    test "create_payment_method/1, update_payment_method/2, delete_payment_method/1  with valid data creates a payment_method",
         %{billing_account: ba} do
      attrs = Enum.into([customer_id: ba.stripe_customer], @valid_attrs)
      assert {:ok, %PaymentMethod{} = payment_method} = Billing.create_payment_method(attrs)

      assert @valid_attrs = payment_method

      assert {:ok, %PaymentMethod{} = payment_method} =
               Billing.update_payment_method(payment_method, @update_attrs)

      assert @update_attrs = payment_method

      # invalid data
      assert {:error, %Ecto.Changeset{}} = Billing.create_payment_method(@invalid_attrs)

      assert {:error, %Ecto.Changeset{}} =
               Billing.update_payment_method(payment_method, @invalid_attrs)

      assert {:ok, %PaymentMethod{}} = Billing.delete_payment_method(payment_method)

      assert_raise Ecto.NoResultsError, fn ->
        Billing.get_payment_method!(payment_method.id)
      end
    end

    test "change_payment_method/1 returns a payment_method changeset", %{billing_account: ba} do
      payment_method = insert(:payment_method, customer_id: ba.stripe_customer)
      assert %Ecto.Changeset{} = Billing.change_payment_method(payment_method)
    end
  end

  describe "plans" do
    @valid_attrs %{name: "Free"}
    @update_attrs %{name: "Legacy"}
    @invalid_attrs %{name: nil}
    test "list_plans/0" do
      plan = insert(:plan)
      assert Billing.list_plans() == [plan]
      assert Billing.get_plan!(plan.id) == plan
      assert Billing.get_plan_by(name: plan.name) == plan
    end
    test "create_plan/1, update_plan/2, delete_plan/1" do
      assert {:ok, %Plan{name: "Free"} = plan} = Billing.create_plan(@valid_attrs)
      assert {:ok, %Plan{name: "Legacy"} = plan} = Billing.update_plan(plan, @update_attrs)
      assert {:ok, %Plan{}} = Billing.delete_plan(plan)

      assert_raise Ecto.NoResultsError, fn ->
        Billing.get_plan!(plan.id)
      end
    end
    test "find_plan/3"
    test "get_plan_by_user/1"
    test "change_plan/1 returns changeset" do
      plan = insert(:plan)
      assert %Ecto.Changeset{} = Billing.change_plan(plan)
    end
    test "legacy_plan/0 returns legacy plan" do
      assert %Plan{name: "Legacy"} = Billing.legacy_plan()
    end
  end
end
