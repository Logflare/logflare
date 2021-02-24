defmodule Logflare.PaymentMethodsTest do
  use Logflare.DataCase
  @moduledoc :skip

  alias Logflare.PaymentMethods

  describe "payment_methods" do
    alias Logflare.PaymentMethods.PaymentMethod

    @valid_attrs %{price_id: "some price_id", stripe_id: "some stripe_id"}
    @update_attrs %{price_id: "some updated price_id", stripe_id: "some updated stripe_id"}
    @invalid_attrs %{price_id: nil, stripe_id: nil}

    def payment_method_fixture(attrs \\ %{}) do
      {:ok, payment_method} =
        attrs
        |> Enum.into(@valid_attrs)
        |> PaymentMethods.create_payment_method()

      payment_method
    end

    test "list_payment_methods/0 returns all payment_methods" do
      payment_method = payment_method_fixture()
      assert PaymentMethods.list_payment_methods() == [payment_method]
    end

    test "get_payment_method!/1 returns the payment_method with given id" do
      payment_method = payment_method_fixture()
      assert PaymentMethods.get_payment_method!(payment_method.id) == payment_method
    end

    test "create_payment_method/1 with valid data creates a payment_method" do
      assert {:ok, %PaymentMethod{} = payment_method} =
               PaymentMethods.create_payment_method(@valid_attrs)

      assert payment_method.price_id == "some price_id"
      assert payment_method.stripe_id == "some stripe_id"
    end

    test "create_payment_method/1 with invalid data returns error changeset" do
      assert {:error, %Ecto.Changeset{}} = PaymentMethods.create_payment_method(@invalid_attrs)
    end

    test "update_payment_method/2 with valid data updates the payment_method" do
      payment_method = payment_method_fixture()

      assert {:ok, %PaymentMethod{} = payment_method} =
               PaymentMethods.update_payment_method(payment_method, @update_attrs)

      assert payment_method.price_id == "some updated price_id"
      assert payment_method.stripe_id == "some updated stripe_id"
    end

    test "update_payment_method/2 with invalid data returns error changeset" do
      payment_method = payment_method_fixture()

      assert {:error, %Ecto.Changeset{}} =
               PaymentMethods.update_payment_method(payment_method, @invalid_attrs)

      assert payment_method == PaymentMethods.get_payment_method!(payment_method.id)
    end

    test "delete_payment_method/1 deletes the payment_method" do
      payment_method = payment_method_fixture()
      assert {:ok, %PaymentMethod{}} = PaymentMethods.delete_payment_method(payment_method)

      assert_raise Ecto.NoResultsError, fn ->
        PaymentMethods.get_payment_method!(payment_method.id)
      end
    end

    test "change_payment_method/1 returns a payment_method changeset" do
      payment_method = payment_method_fixture()
      assert %Ecto.Changeset{} = PaymentMethods.change_payment_method(payment_method)
    end
  end
end
