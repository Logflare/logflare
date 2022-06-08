defmodule Logflare.BillingTest do
  use Logflare.DataCase
  alias Logflare.{User, Billing, Billing.BillingAccount, Billing.PaymentMethod}

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
end
