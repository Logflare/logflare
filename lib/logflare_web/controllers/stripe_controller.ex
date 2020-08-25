defmodule LogflareWeb.StripeController do
  use LogflareWeb, :controller

  require Logger

  alias Logflare.BillingAccounts
  alias Logflare.BillingAccounts.BillingAccount
  alias Logflare.Users
  alias Logflare.Source

  def event(
        conn,
        %{"type" => type, "data" => %{"object" => %{"customer" => customer} = object}} = event
      ) do
    case type do
      "invoice." <> _sub_type ->
        with %BillingAccount{} = ba <-
               BillingAccounts.get_billing_account_by(stripe_customer: customer),
             {:ok, _billing_account} <- BillingAccounts.sync_invoices(ba) do
          ok(conn)
        else
          err ->
            Logger.error("Stripe webhook error: #{type}", %{
              billing: %{webhook_type: type, error_string: inspect(err)}
            })

            conflict(conn)
        end

      "charge.succeeded" ->
        with %BillingAccount{} = ba <-
               BillingAccounts.get_billing_account_by(stripe_customer: customer),
             {:ok, ba} <-
               BillingAccounts.update_billing_account(ba, %{
                 lifetime_plan?: true,
                 lifetime_plan_invoice: object["receipt_url"]
               }) do
          Users.get(ba.user_id)
          |> Source.Supervisor.reset_all_user_sources()

          Logger.info("Lifetime customer created. Event id: #{event["id"]}")

          ok(conn)
        else
          err ->
            Logger.error("Stripe webhook error: #{type}", %{
              billing: %{webhook_type: type, error_string: inspect(err)}
            })

            conflict(conn)
        end

      "customer.subscription" <> sub_type ->
        with %BillingAccount{} = ba <-
               BillingAccounts.get_billing_account_by(stripe_customer: customer),
             {:ok, ba} <- BillingAccounts.sync_subscriptions(ba) do
          Users.get(ba.user_id)
          |> Source.Supervisor.reset_all_user_sources()

          Logger.info("Subscription customer #{sub_type}. Event id: #{event["id"]}")

          ok(conn)
        else
          err ->
            Logger.error("Stripe webhook error: #{type}", %{
              billing: %{webhook_type: type, error_string: inspect(err)}
            })

            conflict(conn)
        end

      _else ->
        not_implimented(conn)
    end
  end

  def event(conn, _params) do
    not_implimented(conn)
  end

  defp ok(conn) do
    conn
    |> json(%{message: "ok"})
  end

  defp conflict(conn) do
    conn
    |> put_status(409)
    |> json(%{message: "conflict"})
  end

  defp not_implimented(conn) do
    conn
    |> put_status(202)
    |> json(%{message: "event type not implimented"})
  end
end
