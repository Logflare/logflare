defmodule Logflare.BillingTest do
  use Logflare.DataCase
  alias Logflare.{User, Billing, Billing.BillingAccount, Billing.PaymentMethod, Billing.Plan}

  alias Logflare.Partners

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
      assert %{} = Billing.get_billing_account_stripe_plan(ba)
      assert %{} = Billing.get_billing_account_stripe_subscription_item(ba)
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

    test "delete_all_payment_methods_by/1 deletes PaymentMethod by keyword", %{
      billing_account: ba
    } do
      payment_method = insert(:payment_method, customer_id: ba.stripe_customer)
      assert {1, _} = Billing.delete_all_payment_methods_by(customer_id: ba.stripe_customer)

      assert_raise Ecto.NoResultsError, fn ->
        Billing.get_payment_method!(payment_method.id)
      end
    end

    test "create_payment_method_with_stripe/1 and delete_payment_method_with_stripe/1 interfaces with stripe",
         %{billing_account: ba} do
      Stripe.PaymentMethod
      |> expect(:attach, 2, fn _ -> {:ok, %{}} end)
      |> expect(:detach, fn _ -> {:ok, %{}} end)

      [_, pm] =
        for n <- 1..2,
            pm_id = "some payment id #{n}" do
          assert {:ok, %PaymentMethod{} = pm} =
                   Billing.create_payment_method_with_stripe(%{
                     "customer_id" => ba.stripe_customer,
                     "stripe_id" => pm_id
                   })

          pm
        end

      assert {:ok, %PaymentMethod{}} = Billing.delete_payment_method_with_stripe(pm)

      assert_raise Ecto.NoResultsError, fn ->
        Billing.get_payment_method!(pm.id)
      end
    end

    test "sync_payment_methods/1 ensures all payment data is correct", %{billing_account: ba} do
      cust_id = ba.stripe_customer

      Stripe.PaymentMethod
      |> expect(:list, 1, fn _ ->
        {:ok,
         %Stripe.List{
           data: [
             %{
               id: "some payment method id",
               customer: cust_id,
               card: %{last4: "1234", exp_month: "02", exp_year: "2022", brand: "visa"}
             }
           ]
         }}
      end)

      assert {:ok,
              [
                %PaymentMethod{
                  customer_id: ^cust_id,
                  stripe_id: "some payment method id",
                  last_four: "1234"
                }
              ]} = Billing.sync_payment_methods(cust_id)

      assert Billing.list_payment_methods_by(customer_id: cust_id) |> length() == 1
    end
  end

  describe "plans" do
    @valid_attrs %{name: "Month", period: "month"}
    @update_attrs %{name: "Legacy", period: "annual"}
    @invalid_attrs %{name: nil}

    test "list_plans/0" do
      plan = insert(:plan)
      assert Billing.list_plans() == [plan]
      assert Billing.get_plan!(plan.id) == plan
      assert Billing.get_plan_by(name: plan.name) == plan
    end

    test "create_plan/1, update_plan/2, delete_plan/1" do
      assert {:ok, %Plan{name: "Month"} = plan} = Billing.create_plan(@valid_attrs)
      assert {:error, %Ecto.Changeset{}} = Billing.create_plan(@invalid_attrs)
      assert {:ok, %Plan{name: "Legacy"} = plan} = Billing.update_plan(plan, @update_attrs)
      assert {:error, %Ecto.Changeset{}} = Billing.update_plan(plan, @invalid_attrs)
      assert {:ok, %Plan{}} = Billing.delete_plan(plan)

      assert_raise Ecto.NoResultsError, fn ->
        Billing.get_plan!(plan.id)
      end
    end

    test "find_plan/3 finds a specific plan" do
      insert(:plan, @valid_attrs)
      insert(:plan, @update_attrs)
      plans = Billing.list_plans()
      assert %Plan{} = Billing.find_plan(plans, "annual", "Legacy")
      assert Billing.find_plan(plans, "month", "Legacy") == nil
    end

    test "get_plan_by_user/1" do
      user = insert(:user, billing_enabled: true)
      assert_raise(RuntimeError, fn -> Billing.get_plan_by_user(user) end)
      insert(:plan, name: "Free")
      insert(:plan, name: "Lifetime")
      # no billing account, return Free
      assert %Plan{name: "Free"} = Billing.get_plan_by_user(user)
      # on lifetime plan
      ba = insert(:billing_account, lifetime_plan: true) |> Repo.preload(:user)
      assert %Plan{name: "Lifetime"} = Billing.get_plan_by_user(ba.user)
      # no stripe subscription
      user_no_stripe = insert(:user, billing_enabled: true)

      ba =
        insert(:billing_account, user: user_no_stripe, stripe_subscriptions: nil)
        |> Repo.preload(:user)

      assert %Plan{name: "Free"} = Billing.get_plan_by_user(ba.user)

      # have billing account
      user_custom = insert(:user, billing_enabled: true)
      plan = insert(:plan, name: "Custom", stripe_id: "stripe-id")

      ba =
        insert(:billing_account, user: user_custom, stripe_plan_id: plan.stripe_id)
        |> Repo.preload(:user)

      assert %Plan{name: "Custom"} = Billing.get_plan_by_user(ba.user)
    end

    test "get_plan_by_user/1 with partner upgrade/downgrade" do
      insert(:plan, name: "Free")
      insert(:plan, name: "Enterprise")
      partner = insert(:partner)
      user = insert(:user, partner: partner)
      assert %Plan{name: "Free"} = Billing.get_plan_by_user(user)
      # upgrade user
      assert {:ok, user} = Partners.upgrade_user(user)

      assert %Plan{name: "Enterprise"} = Billing.get_plan_by_user(user)
    end

    test "change_plan/1 returns changeset" do
      plan = insert(:plan)
      assert %Ecto.Changeset{} = Billing.change_plan(plan)
    end

    test "legacy_plan/0 returns legacy plan" do
      assert %Plan{name: "Legacy"} = Billing.legacy_plan()
    end
  end

  describe "cost_estimate/2" do
    test "calculates an estimate based on the price and the total usage by multiplying the values" do
      assert Billing.cost_estimate(%Plan{price: 10}, 10) == 100
    end
  end
end
