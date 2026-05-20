defmodule LogflareWeb.BillingAccountLive.EditTest do
  use LogflareWeb.ConnCase, async: true

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

  @test_webhook_secret "whsec_test_only_secret_for_testing"

  defp stripe_sign(raw_body) do
    timestamp = System.system_time(:second)
    signed = "#{timestamp}.#{raw_body}"
    hmac = :crypto.mac(:hmac, :sha256, @test_webhook_secret, signed) |> Base.encode16(case: :lower)
    "t=#{timestamp},v1=#{hmac}"
  end

  defp create_billing_account(%{conn: conn}) do
    Stripe.Customer
    |> expect(:create, 1, fn _user -> {:ok, %{id: TestUtils.random_string()}} end)

    assert conn
           |> post(~p"/billing")
           |> redirected_to(302) == ~p"/billing/edit"

    :ok
  end
end
