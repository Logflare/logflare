defmodule Logflare.BillingTest do
  use Logflare.DataCase

  alias Logflare.Billing

  describe "billing_accounts" do
    alias Logflare.Billing.BillingAccount

    @valid_attrs %{latest_successful_stripe_session: %{}, ststripe_customer: %{}}
    @update_attrs %{latest_successful_stripe_session: %{}, ststripe_customer: %{}}
    @invalid_attrs %{latest_successful_stripe_session: nil, ststripe_customer: nil}

    def billing_account_fixture(attrs \\ %{}) do
      {:ok, billing_account} =
        attrs
        |> Enum.into(@valid_attrs)
        |> Billing.create_billing_account()

      billing_account
    end

    test "list_billing_accounts/0 returns all billing_accounts" do
      billing_account = billing_account_fixture()
      assert Billing.list_billing_accounts() == [billing_account]
    end

    test "get_billing_account!/1 returns the billing_account with given id" do
      billing_account = billing_account_fixture()
      assert Billing.get_billing_account!(billing_account.id) == billing_account
    end

    test "create_billing_account/1 with valid data creates a billing_account" do
      assert {:ok, %BillingAccount{} = billing_account} = Billing.create_billing_account(@valid_attrs)
      assert billing_account.latest_successful_stripe_session == %{}
      assert billing_account.ststripe_customer == %{}
    end

    test "create_billing_account/1 with invalid data returns error changeset" do
      assert {:error, %Ecto.Changeset{}} = Billing.create_billing_account(@invalid_attrs)
    end

    test "update_billing_account/2 with valid data updates the billing_account" do
      billing_account = billing_account_fixture()
      assert {:ok, %BillingAccount{} = billing_account} = Billing.update_billing_account(billing_account, @update_attrs)
      assert billing_account.latest_successful_stripe_session == %{}
      assert billing_account.ststripe_customer == %{}
    end

    test "update_billing_account/2 with invalid data returns error changeset" do
      billing_account = billing_account_fixture()
      assert {:error, %Ecto.Changeset{}} = Billing.update_billing_account(billing_account, @invalid_attrs)
      assert billing_account == Billing.get_billing_account!(billing_account.id)
    end

    test "delete_billing_account/1 deletes the billing_account" do
      billing_account = billing_account_fixture()
      assert {:ok, %BillingAccount{}} = Billing.delete_billing_account(billing_account)
      assert_raise Ecto.NoResultsError, fn -> Billing.get_billing_account!(billing_account.id) end
    end

    test "change_billing_account/1 returns a billing_account changeset" do
      billing_account = billing_account_fixture()
      assert %Ecto.Changeset{} = Billing.change_billing_account(billing_account)
    end
  end
end
