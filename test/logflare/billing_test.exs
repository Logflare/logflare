defmodule Logflare.BillingTest do
  use Logflare.DataCase
  alias Logflare.{User, Billing, Billing.BillingAccount}

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
      ba = build(:billing_account) |> IO.inspect()
      assert Billing.get_billing_account_stripe_plan(ba) != nil
      assert Billing.get_billing_account_stripe_subscription_item(ba) != nil
    end
  end
end
