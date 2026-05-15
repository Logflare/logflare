defmodule LogflareWeb.BillingAccountLive.PaymentMethodComponentTest do
  use LogflareWeb.ConnCase, async: false

  alias Logflare.Billing

  setup :set_mimic_from_context

  describe "delete payment method authorization" do
    setup %{conn: conn} do
      insert(:plan, name: "Free")
      insert(:plan, name: "Metered")
      insert(:plan, name: "Metered BYOB")

      user = insert(:user)
      conn = login_user(conn, user)

      Stripe.Customer
      |> expect(:create, 1, fn _user -> {:ok, %{id: TestUtils.random_string()}} end)

      assert conn
             |> post(~p"/billing")
             |> redirected_to(302) == ~p"/billing/edit"

      user = Logflare.Users.get(user.id) |> Logflare.Users.preload_billing_account()
      assert user.billing_account

      {:ok, user: user, conn: conn}
    end

    test "cannot delete a payment method belonging to another billing account", %{
      conn: conn,
      user: user
    } do
      # User A owns one payment method (must have >=1 for UI to render a delete button)
      own_pm =
        insert(:payment_method,
          customer_id: user.billing_account.stripe_customer,
          brand: "visa",
          last_four: "4242",
          exp_month: 12,
          exp_year: 2099
        )

      # Victim has their own billing account and payment method (different stripe_customer)
      victim_user = insert(:user)
      victim_ba = insert(:billing_account, user: victim_user)
      victim_pm = insert(:payment_method, customer_id: victim_ba.stripe_customer)

      {:ok, view, _html} = live(conn, ~p"/billing/edit")

      # Select user A's legit delete button, but override phx-value-id with victim's id
      # — simulating a crafted client payload.
      html =
        view
        |> element("button[phx-click='delete'][phx-value-id='#{own_pm.id}']")
        |> render_click(%{"id" => victim_pm.id})

      assert html =~ "Payment method not found"

      assert Billing.get_payment_method!(victim_pm.id)
      assert Billing.get_payment_method!(own_pm.id)
    end
  end
end
