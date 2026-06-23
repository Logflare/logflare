defmodule LogflareWeb.BillingAccountLive.EditTest do
  use LogflareWeb.ConnCase, async: false

  alias Logflare.Billing

  describe "when user is logged in and billing account is created" do
    setup [:create_plans, :log_in_user, :create_billing_account]

    test "can view billing page when billing account is already created", %{conn: conn} do
      {:ok, view, html} = live(conn, ~p"/billing/edit")

      assert html =~ "~/billing/edit"
      assert has_element?(view, "h1,h2,h3,h4", "Billing Account")
      assert has_element?(view, "p", "You're currently on the Free plan")

      view
      |> form("#usage-form", %{"usage" => %{"days" => "60"}})
      |> render_change()

      assert has_element?(view, "#usage-form")
    end

    test "handles Stripe webhook events", %{conn: conn} do
      billing_account = insert(:billing_account)

      payload_json =
        Jason.encode!(%{
          "id" => "evt_#{TestUtils.random_string()}",
          "object" => "event",
          "type" => "payment_method.attached",
          "data" => %{
            "object" => %{
              "object" => "payment_method",
              "id" => "pm_#{TestUtils.random_string()}",
              "customer" => billing_account.stripe_customer,
              "card" => %{
                "brand" => "visa",
                "exp_month" => "03",
                "exp_year" => "1995",
                "last4" => "4242"
              }
            }
          }
        })

      {:ok, view, html} = live(conn, ~p"/billing/edit")

      refute html =~ "VISA ending in 4242 expires 3/1995"

      assert conn
             |> put_req_header("content-type", "application/json")
             |> put_req_header("stripe-signature", stripe_sign(payload_json))
             |> post(~p"/webhooks/stripe", payload_json)
             |> response(200)

      TestUtils.retry_assert(fn ->
        assert render(view) =~ "VISA ending in 4242 expires 3/1995"
      end)
    end

    test "payment_method.detached webhook removes the method from the live page", %{
      conn: conn,
      user: user
    } do
      stripe_id = "pm_to_detach"

      insert(:payment_method,
        customer_id: user.billing_account.stripe_customer,
        stripe_id: stripe_id,
        brand: "visa",
        last_four: "4242",
        exp_month: 12,
        exp_year: 2099
      )

      {:ok, view, html} = live(conn, ~p"/billing/edit")
      assert html =~ "VISA ending in 4242 expires 12/2099"

      payload_json =
        Jason.encode!(%{
          "id" => "evt_#{TestUtils.random_string()}",
          "object" => "event",
          "type" => "payment_method.detached",
          "data" => %{
            "object" => %{
              "object" => "payment_method",
              "id" => stripe_id,
              "customer" => user.billing_account.stripe_customer
            }
          }
        })

      assert conn
             |> put_req_header("content-type", "application/json")
             |> put_req_header("stripe-signature", stripe_sign(payload_json))
             |> post(~p"/webhooks/stripe", payload_json)
             |> response(200)

      TestUtils.retry_assert(fn ->
        refute render(view) =~ "VISA ending in 4242 expires 12/2099"
      end)
    end
  end

  describe "payment methods rendering" do
    setup [:create_plans, :log_in_user, :create_billing_account]

    test "with no payment methods, shows the empty state with sync + add buttons", %{conn: conn} do
      {:ok, _view, html} = live(conn, ~p"/billing/edit")

      refute html =~ "ending in"
      assert html =~ "Sync payment methods"
      assert html =~ "Add payment method"
      assert html =~ ~s|data-stripe-customer="|
    end

    test "renders each payment method with brand/last_four/expiry", %{conn: conn, user: user} do
      insert(:payment_method,
        customer_id: user.billing_account.stripe_customer,
        stripe_id: "pm_a",
        brand: "visa",
        last_four: "4242",
        exp_month: 12,
        exp_year: 2099
      )

      {:ok, _view, html} = live(conn, ~p"/billing/edit")

      assert html =~ "VISA ending in 4242 expires 12/2099"
    end

    test "default payment method has no 'Make default' button; non-default does", %{
      conn: conn,
      user: user
    } do
      Billing.update_billing_account(user.billing_account, %{default_payment_method: "pm_default"})

      insert(:payment_method,
        customer_id: user.billing_account.stripe_customer,
        stripe_id: "pm_default",
        brand: "visa",
        last_four: "4242",
        exp_month: 12,
        exp_year: 2099
      )

      insert(:payment_method,
        customer_id: user.billing_account.stripe_customer,
        stripe_id: "pm_other",
        brand: "mastercard",
        last_four: "5555",
        exp_month: 1,
        exp_year: 2030
      )

      {:ok, _view, html} = live(conn, ~p"/billing/edit")

      refute html =~ ~s|phx-value-stripe-id="pm_default"|
      assert html =~ ~s|phx-value-stripe-id="pm_other"|
    end
  end

  describe "submitting the payment form" do
    setup [:create_plans, :log_in_user, :create_billing_account]

    test "pushes a `submit` event for the JS hook to consume", %{conn: conn} do
      {:ok, view, _html} = live(conn, ~p"/billing/edit")

      view
      |> form("#payment-form")
      |> render_submit()

      assert_push_event(view, "submit", %{})
    end
  end

  describe "deleting a payment method" do
    setup [:create_plans, :log_in_user, :create_billing_account]

    test "removes it when stripe call succeeds", %{conn: conn, user: user} do
      pm_to_delete =
        insert(:payment_method,
          customer_id: user.billing_account.stripe_customer,
          stripe_id: "pm_delete",
          brand: "visa",
          last_four: "4242",
          exp_month: 12,
          exp_year: 2099
        )

      insert(:payment_method,
        customer_id: user.billing_account.stripe_customer,
        stripe_id: "pm_keep",
        brand: "visa",
        last_four: "1111",
        exp_month: 1,
        exp_year: 2030
      )

      test_pid = self()

      expect(Stripe.PaymentMethod, :detach, fn stripe_id ->
        send(test_pid, {:stripe_detach, stripe_id})
        {:ok, %{id: stripe_id}}
      end)

      {:ok, view, _html} = live(conn, ~p"/billing/edit")

      html =
        view
        |> element("button[phx-click='delete'][phx-value-id='#{pm_to_delete.id}']")
        |> render_click()

      assert html =~ "Payment method deleted!"
      refute html =~ "ending in 4242"
      assert html =~ "ending in 1111"
      assert_received {:stripe_detach, "pm_delete"}
      assert Billing.get_payment_method_by(id: pm_to_delete.id) == nil
    end

    test "shows the stripe error message when detach fails", %{conn: conn, user: user} do
      pm =
        insert(:payment_method,
          customer_id: user.billing_account.stripe_customer,
          stripe_id: "pm_fail",
          brand: "visa",
          last_four: "4242",
          exp_month: 12,
          exp_year: 2099
        )

      insert(:payment_method,
        customer_id: user.billing_account.stripe_customer,
        stripe_id: "pm_keep",
        brand: "visa",
        last_four: "1111",
        exp_month: 1,
        exp_year: 2030
      )

      expect(Stripe.PaymentMethod, :detach, fn _ ->
        {:error, %Stripe.Error{message: "stripe blew up", source: :stripe, code: :api_error}}
      end)

      {:ok, view, _html} = live(conn, ~p"/billing/edit")

      html =
        view
        |> element("button[phx-click='delete'][phx-value-id='#{pm.id}']")
        |> render_click()

      assert html =~ "stripe blew up"
      assert Billing.get_payment_method!(pm.id)
    end

    test "refuses when this is the user's only payment method", %{conn: conn, user: user} do
      pm =
        insert(:payment_method,
          customer_id: user.billing_account.stripe_customer,
          stripe_id: "pm_only",
          brand: "visa",
          last_four: "4242",
          exp_month: 12,
          exp_year: 2099
        )

      {:ok, view, _html} = live(conn, ~p"/billing/edit")

      html =
        view
        |> element("button[phx-click='delete'][phx-value-id='#{pm.id}']")
        |> render_click()

      assert html =~ "You need at least one payment method!"
      assert Billing.get_payment_method!(pm.id)
    end

    test "cannot delete a payment method belonging to another billing account", %{
      conn: conn,
      user: user
    } do
      own_pm =
        insert(:payment_method,
          customer_id: user.billing_account.stripe_customer,
          brand: "visa",
          last_four: "4242",
          exp_month: 12,
          exp_year: 2099
        )

      victim_user = insert(:user)
      victim_ba = insert(:billing_account, user: victim_user)
      victim_pm = insert(:payment_method, customer_id: victim_ba.stripe_customer)

      {:ok, view, _html} = live(conn, ~p"/billing/edit")

      html =
        view
        |> element("button[phx-click='delete'][phx-value-id='#{own_pm.id}']")
        |> render_click(%{"id" => victim_pm.id})

      assert html =~ "Payment method not found"
      assert Billing.get_payment_method!(victim_pm.id)
      assert Billing.get_payment_method!(own_pm.id)
    end
  end

  describe "saving a new payment method (post-Stripe.js callback)" do
    setup [:create_plans, :log_in_user, :create_billing_account]

    test "creates the local record and shows success when stripe attach succeeds", %{
      conn: conn,
      user: user
    } do
      test_pid = self()

      expect(Stripe.PaymentMethod, :attach, fn pm_id, %{customer: cust_id} ->
        send(test_pid, {:stripe_attach, pm_id, cust_id})
        {:ok, %{id: pm_id}}
      end)

      {:ok, view, _html} = live(conn, ~p"/billing/edit")

      params = %{
        "customer_id" => user.billing_account.stripe_customer,
        "stripe_id" => "pm_new",
        "brand" => "visa",
        "last_four" => "9999",
        "exp_month" => 6,
        "exp_year" => 2040
      }

      html =
        view
        |> element("#payment-form")
        |> render_hook("save", params)

      assert html =~ "Payment method created!"
      assert html =~ "VISA ending in 9999 expires 6/2040"
      assert_received {:stripe_attach, "pm_new", _}
      assert Billing.get_payment_method_by(stripe_id: "pm_new")
    end

    test "swallows the error when stripe attach fails", %{conn: conn, user: user} do
      expect(Stripe.PaymentMethod, :attach, fn _, _ ->
        {:error, %Stripe.Error{message: "boom", source: :stripe, code: :api_error}}
      end)

      {:ok, view, _html} = live(conn, ~p"/billing/edit")

      params = %{
        "customer_id" => user.billing_account.stripe_customer,
        "stripe_id" => "pm_failed",
        "brand" => "visa",
        "last_four" => "0000",
        "exp_month" => 6,
        "exp_year" => 2040
      }

      html =
        view
        |> element("#payment-form")
        |> render_hook("save", params)

      refute html =~ "Payment method created!"
      assert Billing.get_payment_method_by(stripe_id: "pm_failed") == nil
    end
  end

  describe "sync button" do
    setup [:create_plans, :log_in_user, :create_billing_account]

    test "replaces local payment methods with the stripe source of truth", %{
      conn: conn,
      user: user
    } do
      # Pre-existing local method that stripe no longer knows about
      insert(:payment_method,
        customer_id: user.billing_account.stripe_customer,
        stripe_id: "pm_stale",
        brand: "amex",
        last_four: "0001",
        exp_month: 1,
        exp_year: 2025
      )

      stripe_pm = %{
        id: "pm_fresh",
        customer: user.billing_account.stripe_customer,
        card: %{brand: "visa", last4: "7777", exp_month: 5, exp_year: 2050}
      }

      expect(Stripe.PaymentMethod, :list, fn %{customer: _, type: "card"} ->
        {:ok, %Stripe.List{data: [stripe_pm]}}
      end)

      expect(Stripe.Subscription, :list, fn _ -> {:ok, %Stripe.List{data: []}} end)

      expect(Stripe.Invoice, :list, fn _ -> {:ok, %Stripe.List{data: []}} end)

      expect(Stripe.Customer, :retrieve, fn _ ->
        {:ok,
         %{
           invoice_settings: %{
             default_payment_method: "pm_fresh",
             custom_fields: nil
           }
         }}
      end)

      {:ok, view, _html} = live(conn, ~p"/billing/edit")

      html =
        view
        |> element("button[phx-click='sync']")
        |> render_click()

      assert html =~ "Payment methods successfully synced!"
      assert html =~ "VISA ending in 7777 expires 5/2050"
      assert Billing.get_payment_method_by(stripe_id: "pm_stale") == nil
      assert Billing.get_payment_method_by(stripe_id: "pm_fresh")
    end

    test "shows the stripe error message when sync fails", %{conn: conn} do
      Stripe.PaymentMethod
      |> expect(:list, fn _ ->
        {:error, %Stripe.Error{message: "stripe is down", source: :stripe, code: :api_error}}
      end)

      {:ok, view, _html} = live(conn, ~p"/billing/edit")

      html =
        view
        |> element("button[phx-click='sync']")
        |> render_click()

      assert html =~ "stripe is down"
      refute html =~ "Payment methods successfully synced!"
    end
  end

  describe "make-default button" do
    setup [:create_plans, :log_in_user, :create_billing_account]

    test "updates billing account when no subscriptions exist", %{conn: conn, user: user} do
      Billing.update_billing_account(user.billing_account, %{stripe_subscriptions: nil})

      insert(:payment_method,
        customer_id: user.billing_account.stripe_customer,
        stripe_id: "pm_default_target",
        brand: "visa",
        last_four: "4242",
        exp_month: 12,
        exp_year: 2099
      )

      insert(:payment_method,
        customer_id: user.billing_account.stripe_customer,
        stripe_id: "pm_other",
        brand: "visa",
        last_four: "1111",
        exp_month: 1,
        exp_year: 2030
      )

      test_pid = self()

      expect(Stripe.Customer, :update, fn customer_id, %{invoice_settings: settings} ->
        send(test_pid, {:stripe_customer_update, customer_id, settings})
        {:ok, %{id: customer_id}}
      end)

      {:ok, view, _html} = live(conn, ~p"/billing/edit")

      html =
        view
        |> element("button[phx-click='make-default'][phx-value-stripe-id='pm_default_target']")
        |> render_click()

      assert html =~ "Default payment method set for your billing account!"
      assert_received {:stripe_customer_update, _, %{default_payment_method: "pm_default_target"}}

      updated = Billing.get_billing_account_by(user_id: user.id)
      assert updated.default_payment_method == "pm_default_target"
    end

    test "also updates each existing subscription", %{conn: conn, user: user} do
      Billing.update_billing_account(user.billing_account, %{
        stripe_subscriptions: %{
          "data" => [
            %{"id" => "sub_1"},
            %{"id" => "sub_2"}
          ]
        }
      })

      insert(:payment_method,
        customer_id: user.billing_account.stripe_customer,
        stripe_id: "pm_target",
        brand: "visa",
        last_four: "4242",
        exp_month: 12,
        exp_year: 2099
      )

      insert(:payment_method,
        customer_id: user.billing_account.stripe_customer,
        stripe_id: "pm_other",
        brand: "visa",
        last_four: "1111",
        exp_month: 1,
        exp_year: 2030
      )

      test_pid = self()

      expect(Stripe.Customer, :update, fn _, _ -> {:ok, %{id: "cus"}} end)

      expect(Stripe.Subscription, :update, 2, fn sub_id, params ->
        send(test_pid, {:stripe_sub_update, sub_id, params})
        {:ok, %{id: sub_id}}
      end)

      {:ok, view, _html} = live(conn, ~p"/billing/edit")

      html =
        view
        |> element("button[phx-click='make-default'][phx-value-stripe-id='pm_target']")
        |> render_click()

      assert html =~ "Default payment method set for 2 subscription(s)!"
      assert_received {:stripe_sub_update, "sub_1", %{default_payment_method: "pm_target"}}
      assert_received {:stripe_sub_update, "sub_2", %{default_payment_method: "pm_target"}}
    end

    test "shows the stripe error message when Stripe.update_customer fails", %{
      conn: conn,
      user: user
    } do
      insert(:payment_method,
        customer_id: user.billing_account.stripe_customer,
        stripe_id: "pm_target",
        brand: "visa",
        last_four: "4242",
        exp_month: 12,
        exp_year: 2099
      )

      insert(:payment_method,
        customer_id: user.billing_account.stripe_customer,
        stripe_id: "pm_other",
        brand: "visa",
        last_four: "1111",
        exp_month: 1,
        exp_year: 2030
      )

      Stripe.Customer
      |> expect(:update, fn _, _ ->
        {:error, %Stripe.Error{message: "card declined", source: :stripe, code: :api_error}}
      end)

      {:ok, view, _html} = live(conn, ~p"/billing/edit")

      html =
        view
        |> element("button[phx-click='make-default'][phx-value-stripe-id='pm_target']")
        |> render_click()

      assert html =~ "card declined"
      refute html =~ "Default payment method set"
    end
  end

  describe "when user is logged in" do
    setup [:create_plans, :log_in_user]

    test "is redirected when visiting billing page if user does not have billing account created",
         %{
           conn: conn
         } do
      conn = get(conn, ~p"/billing/edit")
      assert redirected_to(conn, 302) == ~p"/account/edit#create-a-billing-account"

      assert Phoenix.Flash.get(conn.assigns.flash, :error) ==
               "Create a billing account first!"
    end
  end

  test "redirects to login page when not logged in", %{conn: conn} do
    assert conn
           |> get(~p"/billing/edit")
           |> redirected_to(302) == ~p"/auth/login"
  end

  defp log_in_user(%{conn: conn}) do
    user = insert(:user)

    {:ok, user: user, conn: login_user(conn, user)}
  end

  defp create_plans(_) do
    insert(:plan, name: "Free")
    insert(:plan, name: "Metered")
    insert(:plan, name: "Metered BYOB")

    :ok
  end

  defp create_billing_account(%{user: user}) do
    insert(:billing_account, user: user)
    user = Logflare.Users.get(user.id) |> Logflare.Users.preload_billing_account()
    {:ok, user: user}
  end

  @test_webhook_secret "whsec_test_only_secret_for_testing"

  defp stripe_sign(raw_body) do
    timestamp = System.system_time(:second)
    signed = "#{timestamp}.#{raw_body}"

    hmac =
      :crypto.mac(:hmac, :sha256, @test_webhook_secret, signed) |> Base.encode16(case: :lower)

    "t=#{timestamp},v1=#{hmac}"
  end
end
