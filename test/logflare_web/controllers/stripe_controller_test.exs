defmodule LogflareWeb.StripeWebhookHandlerTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  import ExUnit.CaptureLog

  alias Logflare.Billing

  @test_webhook_secret Application.compile_env!(:logflare, :stripe_webhook_secret)

  setup do
    insert(:plan, name: "Free")
    user = insert(:user)
    insert(:team, user: user)

    billing_account = insert(:billing_account, user: user, stripe_customer: "cus_test123")

    {:ok, user: user, billing_account: billing_account}
  end

  describe "signature verification" do
    test "rejects all requests when no webhook secret is configured", %{conn: conn} do
      Application.put_env(:logflare, :stripe_webhook_secret, nil)

      on_exit(fn ->
        Application.put_env(:logflare, :stripe_webhook_secret, @test_webhook_secret)
      end)

      payload_json = Jason.encode!(stripe_event("invoice.payment_succeeded", invoice_object("cus_test123")))

      # Unsigned request
      assert conn
             |> put_req_header("content-type", "application/json")
             |> post(~p"/webhooks/stripe", payload_json)
             |> response(400)

      # Request signed with the previously-valid secret should also be rejected
      assert build_conn()
             |> put_req_header("content-type", "application/json")
             |> put_req_header("stripe-signature", stripe_sign(payload_json))
             |> post(~p"/webhooks/stripe", payload_json)
             |> response(400)
    end

    test "rejects requests with missing stripe-signature header", %{conn: conn} do
      payload_json = Jason.encode!(stripe_event("invoice.payment_succeeded", invoice_object("cus_test123")))

      conn =
        conn
        |> put_req_header("content-type", "application/json")
        |> post(~p"/webhooks/stripe", payload_json)

      assert response(conn, 400)
    end

    test "rejects requests with invalid stripe-signature", %{conn: conn} do
      payload_json = Jason.encode!(stripe_event("invoice.payment_succeeded", invoice_object("cus_test123")))

      conn =
        conn
        |> put_req_header("content-type", "application/json")
        |> put_req_header("stripe-signature", "t=12345,v1=invalidsignature")
        |> post(~p"/webhooks/stripe", payload_json)

      assert response(conn, 400)
    end

    test "accepts requests with valid stripe-signature", %{conn: conn} do
      expect(Stripe.Invoice, :list, fn %{customer: "cus_test123"} ->
        {:ok, %Stripe.List{data: []}}
      end)

      payload = stripe_event("invoice.payment_succeeded", invoice_object("cus_test123"))
      payload_json = Jason.encode!(payload)
      signature = stripe_sign(payload_json)

      conn =
        conn
        |> put_req_header("content-type", "application/json")
        |> put_req_header("stripe-signature", signature)
        |> post(~p"/webhooks/stripe", payload_json)

      assert response(conn, 200)
    end
  end

  describe "invoice events" do
    test "syncs invoices when billing account exists", %{conn: conn, billing_account: ba} do
      expect(Stripe.Invoice, :list, fn %{customer: "cus_test123"} ->
        {:ok, %Stripe.List{data: []}}
      end)

      conn = post_signed(conn, stripe_event("invoice.payment_succeeded", invoice_object(ba.stripe_customer)))

      assert response(conn, 200)
    end

    test "returns 200 when customer not found", %{conn: conn} do
      conn = post_signed(conn, stripe_event("invoice.payment_succeeded", invoice_object("cus_nonexistent")))

      assert response(conn, 200)
    end

    test "returns error when Stripe API fails", %{conn: conn, billing_account: ba} do
      expect(Stripe.Invoice, :list, fn _ ->
        {:error,
         %Stripe.Error{message: "api error", source: :network, code: :internal_server_error}}
      end)

      log =
        capture_log([level: :error], fn ->
          conn = post_signed(conn, stripe_event("invoice.created", invoice_object(ba.stripe_customer)))
          assert response(conn, 400)
        end)

      assert log =~ "Stripe webhook error: invoice.created"
    end
  end

  describe "charge.succeeded (lifetime plan)" do
    test "creates lifetime customer when charge amount is 50000", %{conn: conn, billing_account: ba} do
      payload = stripe_event("charge.succeeded", charge_object(ba.stripe_customer, 50_000, "https://receipt.url"))
      conn = post_signed(conn, payload)

      assert response(conn, 200)

      updated_ba = Billing.get_billing_account_by(stripe_customer: ba.stripe_customer)
      assert updated_ba.lifetime_plan == true
      assert updated_ba.lifetime_plan_invoice == "https://receipt.url"
    end

    test "returns 200 for non-lifetime charge", %{conn: conn, billing_account: ba} do
      payload = stripe_event("charge.succeeded", charge_object(ba.stripe_customer, 1000, nil))
      conn = post_signed(conn, payload)

      assert response(conn, 200)
    end

    test "returns 200 when customer not found for lifetime charge", %{conn: conn} do
      payload = stripe_event("charge.succeeded", charge_object("cus_nonexistent", 50_000, "https://receipt.url"))
      conn = post_signed(conn, payload)

      assert response(conn, 200)
    end
  end

  describe "customer.subscription events" do
    test "syncs subscriptions when billing account exists", %{conn: conn, billing_account: ba} do
      expect(Stripe.Subscription, :list, fn %{customer: "cus_test123"} ->
        {:ok, %Stripe.List{data: []}}
      end)

      conn = post_signed(conn, stripe_event("customer.subscription.created", subscription_object(ba.stripe_customer)))

      assert response(conn, 200)
    end

    test "returns 200 when customer not found", %{conn: conn} do
      conn = post_signed(conn, stripe_event("customer.subscription.updated", subscription_object("cus_nonexistent")))

      assert response(conn, 200)
    end

    test "returns error when Stripe API fails", %{conn: conn, billing_account: ba} do
      expect(Stripe.Subscription, :list, fn _ ->
        {:error,
         %Stripe.Error{message: "api error", source: :network, code: :internal_server_error}}
      end)

      log =
        capture_log([level: :error], fn ->
          conn = post_signed(conn, stripe_event("customer.subscription.deleted", subscription_object(ba.stripe_customer)))
          assert response(conn, 400)
        end)

      assert log =~ "Stripe webhook error: customer.subscription.deleted"
    end
  end

  describe "payment_method.attached" do
    test "creates payment method when it does not exist", %{conn: conn, billing_account: ba} do
      payload = stripe_event("payment_method.attached", payment_method_object(ba.stripe_customer, "pm_brand_new"))
      conn = post_signed(conn, payload)

      assert response(conn, 200)

      pm = Billing.get_payment_method_by(stripe_id: "pm_brand_new")
      assert pm.customer_id == ba.stripe_customer
      assert pm.brand == "visa"
      assert pm.last_four == "4242"
      assert pm.exp_month == 12
      assert pm.exp_year == 2030
    end

    test "returns 200 when payment method already exists", %{conn: conn, billing_account: ba} do
      insert(:payment_method, stripe_id: "pm_existing", customer_id: ba.stripe_customer)

      payload = stripe_event("payment_method.attached", payment_method_object(ba.stripe_customer, "pm_existing"))
      conn = post_signed(conn, payload)

      assert response(conn, 200)
    end
  end

  describe "payment_method.detached" do
    test "deletes payment method when it exists", %{conn: conn, billing_account: ba} do
      insert(:payment_method, stripe_id: "pm_detach123", customer_id: ba.stripe_customer)

      payload = %{
        "id" => "evt_detach",
        "type" => "payment_method.detached",
        "data" => %{
          "object" => %{"object" => "payment_method", "id" => "pm_detach123"},
          "previous_attributes" => %{"customer" => ba.stripe_customer}
        }
      }

      conn = post_signed(conn, payload)

      assert response(conn, 200)
      assert is_nil(Billing.get_payment_method_by(stripe_id: "pm_detach123"))
    end

    test "returns 200 when payment method not found", %{conn: conn, billing_account: ba} do
      payload = %{
        "id" => "evt_detach",
        "type" => "payment_method.detached",
        "data" => %{
          "object" => %{"object" => "payment_method", "id" => "pm_nonexistent"},
          "previous_attributes" => %{"customer" => ba.stripe_customer}
        }
      }

      conn = post_signed(conn, payload)

      assert response(conn, 200)
    end
  end

  describe "unhandled events" do
    test "returns 200 for unknown event type", %{conn: conn, billing_account: ba} do
      payload = stripe_event("some.unknown.event", invoice_object(ba.stripe_customer))
      conn = post_signed(conn, payload)

      assert response(conn, 200)

      assert Billing.get_billing_account_by(stripe_customer: ba.stripe_customer) ==
               TestUtils.reset_associations(ba)
    end

    test "returns 200 for event with no customer", %{conn: conn} do
      payload = %{
        "id" => "evt_unknown",
        "type" => "unknown.event",
        "data" => %{"object" => %{"object" => "invoice"}}
      }

      conn = post_signed(conn, payload)

      assert response(conn, 200)
    end
  end

  # Builds a properly-shaped Stripe event map with a typed object.
  defp stripe_event(type, object) do
    %{
      "id" => "evt_#{TestUtils.random_string()}",
      "type" => type,
      "data" => %{"object" => object}
    }
  end

  defp invoice_object(customer) do
    %{"object" => "invoice", "id" => "in_#{TestUtils.random_string()}", "customer" => customer}
  end

  defp charge_object(customer, amount, receipt_url) do
    %{
      "object" => "charge",
      "id" => "ch_#{TestUtils.random_string()}",
      "customer" => customer,
      "amount" => amount,
      "receipt_url" => receipt_url
    }
  end

  defp subscription_object(customer) do
    %{"object" => "subscription", "id" => "sub_#{TestUtils.random_string()}", "customer" => customer}
  end

  defp payment_method_object(customer, stripe_id) do
    %{
      "object" => "payment_method",
      "id" => stripe_id,
      "customer" => customer,
      "card" => %{
        "brand" => "visa",
        "exp_month" => 12,
        "exp_year" => 2030,
        "last4" => "4242"
      }
    }
  end

  # Encodes, signs, and posts a Stripe event payload.
  defp post_signed(conn, payload) do
    payload_json = Jason.encode!(payload)
    signature = stripe_sign(payload_json)

    conn
    |> put_req_header("content-type", "application/json")
    |> put_req_header("stripe-signature", signature)
    |> post(~p"/webhooks/stripe", payload_json)
  end

  # Computes a valid Stripe-Signature header value for the given raw body.
  defp stripe_sign(raw_body, secret \\ @test_webhook_secret) do
    timestamp = System.system_time(:second)
    signed = "#{timestamp}.#{raw_body}"
    hmac = :crypto.mac(:hmac, :sha256, secret, signed) |> Base.encode16(case: :lower)
    "t=#{timestamp},v1=#{hmac}"
  end
end
