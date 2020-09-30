defmodule LogflareWeb.StripeController do
  use LogflareWeb, :controller

  require Logger

  alias Logflare.BillingAccounts
  alias Logflare.BillingAccounts.BillingAccount

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
          nil = err ->
            message = "customer not found: #{customer}"

            Logger.error("Stripe webhook error: #{type}", %{
              billing: %{
                webhook_type: type,
                error_string: inspect(err),
                customer: customer,
                message: message
              }
            })

            conflict(conn, message)

          err ->
            Logger.error("Stripe webhook error: #{type}", %{
              billing: %{webhook_type: type, error_string: inspect(err), customer: customer}
            })

            conflict(conn)
        end

      "charge.succeeded" ->
        if lifetime_plan_charge?(event) do
          with %BillingAccount{} = ba <-
                 BillingAccounts.get_billing_account_by(stripe_customer: customer),
               {:ok, _ba} <-
                 BillingAccounts.update_billing_account(ba, %{
                   lifetime_plan?: true,
                   lifetime_plan_invoice: object["receipt_url"]
                 }) do
            Logger.info("Lifetime customer created. Event id: #{event["id"]}")

            ok(conn)
          else
            nil = err ->
              message = "customer not found: #{customer}"

              Logger.error("Stripe webhook error: #{type}", %{
                billing: %{
                  webhook_type: type,
                  error_string: inspect(err),
                  customer: customer,
                  message: message
                }
              })

              conflict(conn, message)

            err ->
              Logger.error("Stripe webhook error: #{type}", %{
                billing: %{webhook_type: type, error_string: inspect(err), customer: customer}
              })

              conflict(conn)
          end
        else
          not_implimented(conn)
        end

      "customer.subscription" <> sub_type ->
        with %BillingAccount{} = ba <-
               BillingAccounts.get_billing_account_by(stripe_customer: customer),
             {:ok, _ba} <- BillingAccounts.sync_subscriptions(ba) do
          Logger.info("Subscription customer #{sub_type}. Event id: #{event["id"]}")

          ok(conn)
        else
          nil = err ->
            message = "customer not found: #{customer}"

            Logger.error("Stripe webhook error: #{type}", %{
              billing: %{
                webhook_type: type,
                error_string: inspect(err),
                customer: customer,
                message: message
              }
            })

            conflict(conn, message)

          err ->
            Logger.error("Stripe webhook error: #{type}", %{
              billing: %{webhook_type: type, error_string: inspect(err), customer: customer}
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

  defp conflict(conn, message \\ "conflict") do
    conn
    |> put_status(409)
    |> json(%{message: message})
  end

  defp not_implimented(conn) do
    conn
    |> put_status(202)
    |> json(%{message: "event type not implimented"})
  end

  defp lifetime_plan_charge?(event) do
    event["data"]["object"]["amount"] == 50000
  end
end
