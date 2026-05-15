defmodule LogflareWeb.BillingControllerTest do
  use LogflareWeb.ConnCase, async: true

  import ExUnit.CaptureLog

  alias Logflare.Billing
  alias Logflare.Sources.Source

  setup do
    insert(:plan, name: "Free")
    user = insert(:user)
    insert(:team, user: user)

    {:ok, user: user}
  end

  describe "create/2" do
    test "creates billing account and stripe customer", %{conn: conn, user: user} do
      expect(Stripe.Customer, :create, fn %{name: _, email: _} ->
        {:ok, %Stripe.Customer{id: "cus_new"}}
      end)

      expect(Source.Supervisor, :reset_all_user_sources, fn _ -> :ok end)

      conn = conn |> login_user(user) |> post(~p"/billing")

      assert redirected_to(conn) == "/billing/edit"
      assert Phoenix.Flash.get(conn.assigns.flash, :info) =~ "Billing account created!"

      ba = Billing.get_billing_account_by(user_id: user.id)
      assert ba.stripe_customer == "cus_new"
    end

    test "returns error when stripe customer creation fails", %{conn: conn, user: user} do
      expect(Stripe.Customer, :create, fn _ ->
        {:error, %Stripe.Error{message: "api error", source: :network, code: :api_error}}
      end)

      log =
        capture_log([level: :error], fn ->
          conn = conn |> login_user(user) |> post(~p"/billing")

          assert redirected_to(conn) =~ "/account/edit"
          assert Phoenix.Flash.get(conn.assigns.flash, :error) =~ "Something went wrong"
        end)

      assert log =~ "Billing error"
      assert log =~ "api error"
    end

    test "deletes stripe customer when billing account creation fails", %{conn: conn, user: user} do
      insert(:billing_account, user: user)

      expect(Stripe.Customer, :create, fn _ ->
        {:ok, %Stripe.Customer{id: "cus_to_delete"}}
      end)

      expect(Stripe.Customer, :delete, fn "cus_to_delete" ->
        {:ok, %Stripe.Customer{id: "cus_to_delete"}}
      end)

      log =
        capture_log([level: :error], fn ->
          conn = conn |> login_user(user) |> post(~p"/billing")

          assert redirected_to(conn) =~ "/account/edit"
          assert Phoenix.Flash.get(conn.assigns.flash, :error) =~ "Something went wrong"
        end)

      assert log =~ "Billing error"
      assert log =~ "has already been taken"
    end
  end

  describe "delete/2" do
    test "deletes billing account and stripe customer", %{conn: conn, user: user} do
      insert(:billing_account, user: user, stripe_customer: "cus_test123")

      expect(Source.Supervisor, :reset_all_user_sources, fn _ -> :ok end)

      expect(Stripe.Customer, :delete, fn "cus_test123" ->
        {:ok, %Stripe.Customer{id: "cus_test123"}}
      end)

      conn = conn |> login_user(user) |> delete(~p"/billing")

      assert redirected_to(conn) == "/dashboard"
      assert Phoenix.Flash.get(conn.assigns.flash, :info) =~ "Billing account deleted!"
    end

    test "returns error when delete fails", %{conn: conn, user: user} do
      insert(:billing_account, user: user, stripe_customer: "cus_test123")

      expect(Source.Supervisor, :reset_all_user_sources, fn _ -> :ok end)

      expect(Stripe.Customer, :delete, fn _ ->
        {:error, %Stripe.Error{message: "api error", source: :network, code: :api_error}}
      end)

      log =
        capture_log([level: :error], fn ->
          conn = conn |> login_user(user) |> delete(~p"/billing")

          assert redirected_to(conn) == "/dashboard"
          assert Phoenix.Flash.get(conn.assigns.flash, :error) =~ "Something went wrong"
        end)

      assert log =~ "Billing error"
      assert log =~ "api error"
    end
  end

  describe "confirm_subscription/2 - standard" do
    test "renders confirm page for standard subscription", %{conn: conn, user: user} do
      plan = insert(:plan, name: "Standard", stripe_id: "plan_standard", type: "standard")

      insert(:billing_account,
        user: user,
        stripe_customer: "cus_test123",
        stripe_subscriptions: nil
      )

      expect(Stripe.Session, :create, fn _ ->
        {:ok, %Stripe.Session{id: "cs_test123"}}
      end)

      conn =
        conn
        |> login_user(user)
        |> get(~p"/billing/subscription/confirm", %{"stripe_id" => plan.stripe_id})

      assert html_response(conn, 200) =~ "Redirecting"
    end

    test "returns error when user already has subscription", %{conn: conn, user: user} do
      plan = insert(:plan, name: "Standard", stripe_id: "plan_standard", type: "standard")

      insert(:billing_account,
        user: user,
        stripe_customer: "cus_test123",
        stripe_subscriptions: %{"data" => [%{"id" => "sub_existing"}]}
      )

      conn =
        conn
        |> login_user(user)
        |> get(~p"/billing/subscription/confirm", %{"stripe_id" => plan.stripe_id})

      assert redirected_to(conn) == "/billing/edit"
      assert Phoenix.Flash.get(conn.assigns.flash, :error) =~ "delete your current subscription"
    end
  end

  describe "confirm_subscription/2 - payment mode" do
    test "renders confirm page for payment subscription", %{conn: conn, user: user} do
      plan = insert(:plan, name: "Payment", stripe_id: "plan_payment", type: "standard")

      insert(:billing_account,
        user: user,
        stripe_customer: "cus_test123",
        stripe_subscriptions: nil
      )

      expect(Stripe.Session, :create, fn _ ->
        {:ok, %Stripe.Session{id: "cs_payment"}}
      end)

      conn =
        conn
        |> login_user(user)
        |> get(~p"/billing/subscription/confirm", %{
          "stripe_id" => plan.stripe_id,
          "mode" => "payment"
        })

      assert html_response(conn, 200) =~ "Redirecting"
    end
  end

  describe "confirm_subscription/2 - metered type" do
    test "renders confirm page for metered subscription", %{conn: conn, user: user} do
      plan = insert(:plan, name: "Metered", stripe_id: "plan_metered", type: "metered")

      insert(:billing_account,
        user: user,
        stripe_customer: "cus_test123",
        stripe_subscriptions: nil
      )

      expect(Stripe.Session, :create, fn _ ->
        {:ok, %Stripe.Session{id: "cs_metered"}}
      end)

      conn =
        conn
        |> login_user(user)
        |> get(~p"/billing/subscription/confirm", %{
          "stripe_id" => plan.stripe_id,
          "type" => "metered"
        })

      assert html_response(conn, 200) =~ "Redirecting"
    end

    test "returns error for metered subscription with lifetime plan", %{conn: conn, user: user} do
      plan = insert(:plan, name: "Metered", stripe_id: "plan_metered", type: "metered")

      insert(:billing_account,
        user: user,
        stripe_customer: "cus_test123",
        stripe_subscriptions: nil,
        lifetime_plan: true
      )

      conn =
        conn
        |> login_user(user)
        |> get(~p"/billing/subscription/confirm", %{
          "stripe_id" => plan.stripe_id,
          "type" => "metered"
        })

      assert redirected_to(conn) == "/billing/edit"
      assert Phoenix.Flash.get(conn.assigns.flash, :error) =~ "delete your current subscription"
    end
  end

  describe "confirm_subscription/2 - without billing account" do
    test "creates billing account then confirms subscription", %{conn: conn, user: user} do
      plan = insert(:plan, name: "Standard", stripe_id: "plan_standard", type: "standard")

      expect(Stripe.Customer, :create, fn %{name: _, email: _} ->
        {:ok, %Stripe.Customer{id: "cus_new"}}
      end)

      expect(Source.Supervisor, :reset_all_user_sources, fn _ -> :ok end)

      expect(Stripe.Session, :create, fn _ ->
        {:ok, %Stripe.Session{id: "cs_test123"}}
      end)

      conn =
        conn
        |> login_user(user)
        |> get(~p"/billing/subscription/confirm", %{"stripe_id" => plan.stripe_id})

      assert html_response(conn, 200) =~ "Redirecting"

      ba = Billing.get_billing_account_by(user_id: user.id)
      assert ba.stripe_customer == "cus_new"
    end
  end

  describe "change_subscription/2" do
    test "changes subscription when user has one", %{conn: conn, user: user} do
      current_plan =
        insert(:plan, name: "Standard", stripe_id: "plan_standard", type: "standard")

      target_plan = insert(:plan, name: "Metered", stripe_id: "plan_metered", type: "metered")

      insert(:billing_account,
        user: user,
        stripe_customer: "cus_test123",
        stripe_plan_id: current_plan.stripe_id
      )

      expect(Stripe.Subscription, :update, fn _id, _params ->
        {:ok, %Stripe.Subscription{id: "sub_updated"}}
      end)

      conn =
        conn
        |> login_user(user)
        |> get(~p"/billing/subscription/change", %{
          "plan" => to_string(target_plan.id),
          "type" => "metered"
        })

      assert redirected_to(conn) == "/billing/edit"
      assert Phoenix.Flash.get(conn.assigns.flash, :info) =~ "Plan successfully changed!"
    end

    test "returns error when user has no subscription", %{conn: conn, user: user} do
      current_plan =
        insert(:plan, name: "Standard", stripe_id: "plan_standard", type: "standard")

      target_plan = insert(:plan, name: "Metered", stripe_id: "plan_metered", type: "metered")

      insert(:billing_account,
        user: user,
        stripe_customer: "cus_test123",
        stripe_plan_id: current_plan.stripe_id,
        stripe_subscriptions: nil
      )

      conn =
        conn
        |> login_user(user)
        |> get(~p"/billing/subscription/change", %{
          "plan" => to_string(target_plan.id),
          "type" => "metered"
        })

      assert redirected_to(conn) == "/billing/edit"
      assert Phoenix.Flash.get(conn.assigns.flash, :error) =~ "need a subscription"
    end
  end

  describe "portal/2" do
    test "redirects to stripe billing portal", %{conn: conn, user: user} do
      insert(:billing_account, user: user, stripe_customer: "cus_test123")

      expect(Stripe.BillingPortal.Session, :create, fn _ ->
        {:ok, %Stripe.BillingPortal.Session{url: "https://billing.stripe.com/session/test"}}
      end)

      conn =
        conn
        |> login_user(user)
        |> get(~p"/billing/subscription/manage")

      assert redirected_to(conn) == "https://billing.stripe.com/session/test"
    end

    test "returns error when portal session creation fails", %{conn: conn, user: user} do
      insert(:billing_account, user: user, stripe_customer: "cus_test123")

      expect(Stripe.BillingPortal.Session, :create, fn _ ->
        {:error, %Stripe.Error{message: "api error", source: :network, code: :api_error}}
      end)

      log =
        capture_log([level: :error], fn ->
          conn =
            conn
            |> login_user(user)
            |> get(~p"/billing/subscription/manage")

          assert redirected_to(conn) == "/billing/edit"
          assert Phoenix.Flash.get(conn.assigns.flash, :error) =~ "Something went wrong"
        end)

      assert log =~ "Billing error"
      assert log =~ "api error"
    end
  end

  describe "update_payment_details/2" do
    test "renders confirm page when user has subscription", %{conn: conn, user: user} do
      insert(:billing_account,
        user: user,
        stripe_customer: "cus_test123",
        stripe_subscriptions: %{
          "data" => [%{"id" => "sub_test", "items" => %{"data" => [%{"id" => "si_test"}]}}]
        }
      )

      expect(Stripe.Session, :create, fn _ ->
        {:ok, %Stripe.Session{id: "cs_setup"}}
      end)

      conn =
        conn
        |> login_user(user)
        |> get(~p"/billing/subscription/confirm/change")

      assert html_response(conn, 200) =~ "Redirecting"
    end

    test "returns error when user has no subscription", %{conn: conn, user: user} do
      insert(:billing_account,
        user: user,
        stripe_customer: "cus_test123",
        stripe_subscriptions: nil
      )

      conn =
        conn
        |> login_user(user)
        |> get(~p"/billing/subscription/confirm/change")

      assert redirected_to(conn) == "/billing/edit"
      assert Phoenix.Flash.get(conn.assigns.flash, :error) =~ "subscribe first"
    end
  end

  describe "unsubscribe/2" do
    test "deletes subscription and syncs billing account", %{conn: conn, user: user} do
      insert(:billing_account,
        user: user,
        stripe_customer: "cus_test123",
        stripe_subscriptions: %{
          "data" => [
            %{
              "id" => "sub_test123",
              "plan" => %{"id" => "plan_free"},
              "items" => %{"data" => [%{"id" => "si_test"}]}
            }
          ]
        }
      )

      expect(Stripe.Subscription, :delete, fn "sub_test123" ->
        {:ok, %Stripe.Subscription{id: "sub_test123"}}
      end)

      expect(Stripe.Subscription, :list, fn %{customer: "cus_test123"} ->
        {:ok, %Stripe.List{data: []}}
      end)

      expect(Source.Supervisor, :reset_all_user_sources, fn _ -> :ok end)

      conn =
        conn
        |> login_user(user)
        |> delete(~p"/billing/subscription", %{"id" => "sub_test123"})

      assert redirected_to(conn) == "/billing/edit"
      assert Phoenix.Flash.get(conn.assigns.flash, :info) =~ "Subscription deleted!"
    end

    test "returns error when subscription not found", %{conn: conn, user: user} do
      insert(:billing_account,
        user: user,
        stripe_customer: "cus_test123",
        stripe_subscriptions: %{"data" => [%{"id" => "sub_other"}]}
      )

      conn =
        conn
        |> login_user(user)
        |> delete(~p"/billing/subscription", %{"id" => "sub_nonexistent"})

      assert redirected_to(conn) == "/billing/edit"
      assert Phoenix.Flash.get(conn.assigns.flash, :error) =~ "Subscription not found"
    end
  end

  describe "update_credit_card_success/2" do
    test "updates payment method and billing account", %{conn: conn, user: user} do
      insert(:billing_account, user: user, stripe_customer: "cus_test123")

      stripe_session = %Stripe.Session{id: "cs_test123"}

      expect(Stripe.Event, :list, fn _ ->
        {:ok,
         %Stripe.List{
           data: [
             %Stripe.Event{
               data: %{
                 object: %Stripe.Session{id: "cs_test123", setup_intent: "seti_test"}
               }
             }
           ]
         }}
      end)

      expect(Stripe.SetupIntent, :retrieve, fn "seti_test", %{} ->
        {:ok, %Stripe.SetupIntent{id: "seti_test", payment_method: "pm_new"}}
      end)

      expect(Stripe.Customer, :update, fn "cus_test123",
                                          %{
                                            invoice_settings: %{
                                              default_payment_method: "pm_new"
                                            }
                                          } ->
        {:ok, %Stripe.Customer{id: "cus_test123"}}
      end)

      conn =
        conn
        |> login_user(user)
        |> put_session(:stripe_session, stripe_session)
        |> get(~p"/billing/subscription/updated-payment-method")

      assert redirected_to(conn) == "/billing/edit"
      assert Phoenix.Flash.get(conn.assigns.flash, :info) =~ "Payment method updated!"
    end
  end

  describe "success/2" do
    test "marks subscription as created", %{conn: conn, user: user} do
      insert(:billing_account, user: user, stripe_customer: "cus_test123")

      stripe_session = %Stripe.Session{id: "cs_test123"}

      expect(Stripe.Event, :list, fn _ ->
        {:ok,
         %Stripe.List{
           data: [
             %Stripe.Event{
               data: %{object: %Stripe.Session{id: "cs_test123"}}
             }
           ]
         }}
      end)

      expect(Source.Supervisor, :reset_all_user_sources, fn _ -> :ok end)

      conn =
        conn
        |> login_user(user)
        |> put_session(:stripe_session, stripe_session)
        |> get(~p"/billing/subscription/subscribed")

      assert redirected_to(conn) == "/billing/edit"
      assert Phoenix.Flash.get(conn.assigns.flash, :info) =~ "Subscription created!"
    end
  end

  describe "sync/2" do
    test "syncs billing account", %{conn: conn, user: user} do
      insert(:billing_account, user: user, stripe_customer: "cus_test123")

      expect(Stripe.Subscription, :list, fn %{customer: "cus_test123"} ->
        {:ok, %Stripe.List{data: []}}
      end)

      expect(Stripe.Invoice, :list, fn %{customer: "cus_test123"} ->
        {:ok, %Stripe.List{data: []}}
      end)

      expect(Stripe.Customer, :retrieve, fn "cus_test123" ->
        {:ok,
         %Stripe.Customer{
           id: "cus_test123",
           invoice_settings: %{default_payment_method: nil, custom_fields: nil}
         }}
      end)

      conn =
        conn
        |> login_user(user)
        |> get(~p"/billing/sync")

      assert redirected_to(conn) == "/billing/edit"
      assert Phoenix.Flash.get(conn.assigns.flash, :info) =~ "Billing account synced!"
    end

    test "returns error when sync fails", %{conn: conn, user: user} do
      insert(:billing_account, user: user, stripe_customer: "cus_test123")

      expect(Stripe.Subscription, :list, fn _ ->
        {:error, %Stripe.Error{message: "api error", source: :network, code: :api_error}}
      end)

      log =
        capture_log([level: :error], fn ->
          conn =
            conn
            |> login_user(user)
            |> get(~p"/billing/sync")

          assert redirected_to(conn) == "/billing/edit"
          assert Phoenix.Flash.get(conn.assigns.flash, :error) =~ "Something went wrong"
        end)

      assert log =~ "Billing error"
      assert log =~ "api error"
    end
  end

  describe "abandoned/2" do
    test "shows abandoned flash message", %{conn: conn, user: user} do
      insert(:billing_account, user: user, stripe_customer: "cus_test123")

      conn =
        conn
        |> login_user(user)
        |> get(~p"/billing/subscription/abandoned")

      assert redirected_to(conn) == "/billing/edit"
      assert Phoenix.Flash.get(conn.assigns.flash, :error) =~ "Abandoned!"
    end
  end
end
