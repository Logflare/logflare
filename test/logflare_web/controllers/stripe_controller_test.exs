defmodule LogflareWeb.StripeControllerTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  import ExUnit.CaptureLog

  alias Logflare.Billing

  setup do
    insert(:plan, name: "Free")
    user = insert(:user)
    insert(:team, user: user)

    billing_account = insert(:billing_account, user: user, stripe_customer: "cus_test123")

    {:ok, user: user, billing_account: billing_account}
  end

  describe "event/2 - invoice events" do
    test "syncs invoices when billing account exists", %{conn: conn, billing_account: ba} do
      expect(Stripe.Invoice, :list, fn %{customer: "cus_test123"} ->
        {:ok, %Stripe.List{data: []}}
      end)

      payload = stripe_event("invoice.payment_succeeded", %{"customer" => ba.stripe_customer})

      conn =
        conn
        |> put_req_header("content-type", "application/json")
        |> post(~p"/webhooks/stripe", payload)

      assert json_response(conn, 200) == %{"message" => "ok"}
    end

    test "returns ok with message when customer not found", %{conn: conn} do
      payload = stripe_event("invoice.payment_succeeded", %{"customer" => "cus_nonexistent"})

      conn =
        conn
        |> put_req_header("content-type", "application/json")
        |> post(~p"/webhooks/stripe", payload)

      assert %{"message" => message} = json_response(conn, 200)
      assert message =~ "customer not found"
    end

    test "returns conflict when Stripe API fails", %{conn: conn, billing_account: ba} do
      expect(Stripe.Invoice, :list, fn _ ->
        {:error,
         %Stripe.Error{message: "api error", source: :network, code: :internal_server_error}}
      end)

      payload = stripe_event("invoice.created", %{"customer" => ba.stripe_customer})

      log =
        capture_log([level: :error], fn ->
          conn =
            conn
            |> put_req_header("content-type", "application/json")
            |> post(~p"/webhooks/stripe", payload)

          assert json_response(conn, 202) == %{"message" => "conflict"}
        end)

      assert log =~ "Stripe webhook error: invoice.created"
    end
  end

  describe "event/2 - charge.succeeded (lifetime plan)" do
    test "creates lifetime customer when charge amount is 50000", %{
      conn: conn,
      billing_account: ba
    } do
      payload =
        stripe_event("charge.succeeded", %{
          "customer" => ba.stripe_customer,
          "amount" => 50_000,
          "receipt_url" => "https://receipt.url"
        })

      conn =
        conn
        |> put_req_header("content-type", "application/json")
        |> post(~p"/webhooks/stripe", payload)

      assert json_response(conn, 200) == %{"message" => "ok"}

      updated_ba = Billing.get_billing_account_by(stripe_customer: ba.stripe_customer)
      assert updated_ba.lifetime_plan == true
      assert updated_ba.lifetime_plan_invoice == "https://receipt.url"
    end

    test "returns not implemented for non-lifetime charge", %{
      conn: conn,
      billing_account: ba
    } do
      payload =
        stripe_event("charge.succeeded", %{"customer" => ba.stripe_customer, "amount" => 1000})

      conn =
        conn
        |> put_req_header("content-type", "application/json")
        |> post(~p"/webhooks/stripe", payload)

      assert json_response(conn, 202) == %{"message" => "event type not implimented"}
    end

    test "returns ok with message when customer not found for lifetime charge", %{conn: conn} do
      payload =
        stripe_event("charge.succeeded", %{
          "customer" => "cus_nonexistent",
          "amount" => 50_000,
          "receipt_url" => "https://receipt.url"
        })

      conn =
        conn
        |> put_req_header("content-type", "application/json")
        |> post(~p"/webhooks/stripe", payload)

      assert %{"message" => message} = json_response(conn, 200)
      assert message =~ "customer not found"
    end
  end

  describe "event/2 - customer.subscription events" do
    test "syncs subscriptions when billing account exists", %{conn: conn, billing_account: ba} do
      expect(Stripe.Subscription, :list, fn %{customer: "cus_test123"} ->
        {:ok, %Stripe.List{data: []}}
      end)

      payload = stripe_event("customer.subscription.created", %{"customer" => ba.stripe_customer})

      conn =
        conn
        |> put_req_header("content-type", "application/json")
        |> post(~p"/webhooks/stripe", payload)

      assert json_response(conn, 200) == %{"message" => "ok"}
    end

    test "returns ok with message when customer not found", %{conn: conn} do
      payload = stripe_event("customer.subscription.updated", %{"customer" => "cus_nonexistent"})

      conn =
        conn
        |> put_req_header("content-type", "application/json")
        |> post(~p"/webhooks/stripe", payload)

      assert %{"message" => message} = json_response(conn, 200)
      assert message =~ "customer not found"
    end

    test "returns conflict when Stripe API fails", %{conn: conn, billing_account: ba} do
      expect(Stripe.Subscription, :list, fn _ ->
        {:error,
         %Stripe.Error{message: "api error", source: :network, code: :internal_server_error}}
      end)

      payload = stripe_event("customer.subscription.deleted", %{"customer" => ba.stripe_customer})

      log =
        capture_log([level: :error], fn ->
          conn =
            conn
            |> put_req_header("content-type", "application/json")
            |> post(~p"/webhooks/stripe", payload)

          assert json_response(conn, 202) == %{"message" => "conflict"}
        end)

      assert log =~ "Stripe webhook error: customer.subscription.deleted"
    end
  end

  describe "event/2 - payment_method.attached" do
    test "creates payment method when it does not exist", %{conn: conn, billing_account: ba} do
      payload =
        stripe_event("payment_method.attached", %{
          "customer" => ba.stripe_customer,
          "id" => "pm_brand_new",
          "card" => %{
            "brand" => "visa",
            "exp_month" => 12,
            "exp_year" => 2030,
            "last4" => "4242"
          }
        })

      conn =
        conn
        |> put_req_header("content-type", "application/json")
        |> post(~p"/webhooks/stripe", payload)

      assert json_response(conn, 200) == %{"message" => "ok"}

      pm = Billing.get_payment_method_by(stripe_id: "pm_brand_new")
      assert pm.customer_id == ba.stripe_customer
      assert pm.brand == "visa"
      assert pm.last_four == "4242"
      assert pm.exp_month == 12
      assert pm.exp_year == 2030
    end

    test "returns conflict when payment method already exists", %{conn: conn, billing_account: ba} do
      insert(:payment_method, stripe_id: "pm_existing", customer_id: ba.stripe_customer)

      payload =
        stripe_event("payment_method.attached", %{
          "customer" => ba.stripe_customer,
          "id" => "pm_existing",
          "card" => %{
            "brand" => "visa",
            "exp_month" => 12,
            "exp_year" => 2030,
            "last4" => "4242"
          }
        })

      conn =
        conn
        |> put_req_header("content-type", "application/json")
        |> post(~p"/webhooks/stripe", payload)

      assert json_response(conn, 202) == %{"message" => "conflict"}
    end
  end

  describe "event/2 - unhandled events" do
    test "returns not implemented for unknown event with customer field", %{
      conn: conn,
      billing_account: ba
    } do
      payload = stripe_event("some.unknown.event", %{"customer" => ba.stripe_customer})

      conn =
        conn
        |> put_req_header("content-type", "application/json")
        |> post(~p"/webhooks/stripe", payload)

      assert json_response(conn, 202) == %{"message" => "event type not implimented"}

      assert Billing.get_billing_account_by(stripe_customer: ba.stripe_customer) ==
               TestUtils.reset_associations(ba)
    end

    test "returns not implemented for unknown event in previous_attributes clause", %{
      conn: conn,
      billing_account: ba
    } do
      payload = %{
        "id" => "evt_unknown",
        "type" => "some.unknown.event",
        "data" => %{
          "object" => %{"id" => "obj_123"},
          "previous_attributes" => %{"customer" => ba.stripe_customer}
        }
      }

      conn =
        conn
        |> put_req_header("content-type", "application/json")
        |> post(~p"/webhooks/stripe", payload)

      assert json_response(conn, 202) == %{"message" => "event type not implimented"}

      assert Billing.get_billing_account_by(stripe_customer: ba.stripe_customer) ==
               TestUtils.reset_associations(ba)
    end

    test "returns not implemented for completely unhandled event", %{
      conn: conn,
      billing_account: ba
    } do
      payload = %{
        "id" => "evt_unknown",
        "type" => "unknown.event",
        "data" => %{"object" => %{}}
      }

      conn =
        conn
        |> put_req_header("content-type", "application/json")
        |> post(~p"/webhooks/stripe", payload)

      assert json_response(conn, 202) == %{"message" => "event type not implimented"}

      assert Billing.get_billing_account_by(stripe_customer: ba.stripe_customer) ==
               TestUtils.reset_associations(ba)
    end
  end

  describe "event/2 - payment_method.detached" do
    test "deletes payment method when it exists", %{conn: conn, billing_account: ba} do
      insert(:payment_method, stripe_id: "pm_detach123", customer_id: ba.stripe_customer)

      payload = %{
        "id" => "evt_detach",
        "type" => "payment_method.detached",
        "data" => %{
          "object" => %{"id" => "pm_detach123"},
          "previous_attributes" => %{"customer" => ba.stripe_customer}
        }
      }

      conn =
        conn
        |> put_req_header("content-type", "application/json")
        |> post(~p"/webhooks/stripe", payload)

      assert json_response(conn, 200) == %{"message" => "ok"}
      assert is_nil(Billing.get_payment_method_by(stripe_id: "pm_detach123"))
    end

    test "returns conflict when payment method not found", %{conn: conn, billing_account: ba} do
      payload = %{
        "id" => "evt_detach",
        "type" => "payment_method.detached",
        "data" => %{
          "object" => %{"id" => "pm_nonexistent"},
          "previous_attributes" => %{"customer" => ba.stripe_customer}
        }
      }

      conn =
        conn
        |> put_req_header("content-type", "application/json")
        |> post(~p"/webhooks/stripe", payload)

      assert json_response(conn, 202) == %{"message" => "conflict"}
    end
  end

  describe "event/2 - customer event with invoice_settings" do
    test "returns not implemented", %{conn: conn} do
      payload = %{
        "id" => "evt_customer",
        "type" => "customer.updated",
        "data" => %{
          "object" => %{
            "id" => "cus_test123",
            "invoice_settings" => %{"default_payment_method" => "pm_123"}
          }
        }
      }

      conn =
        conn
        |> put_req_header("content-type", "application/json")
        |> post(~p"/webhooks/stripe", payload)

      assert json_response(conn, 202) == %{"message" => "event type not implimented"}
    end
  end

  defp stripe_event(type, object) do
    %{
      "id" => "evt_#{TestUtils.random_string()}",
      "type" => type,
      "data" => %{
        "object" => object
      }
    }
  end
end
