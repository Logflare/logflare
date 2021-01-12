defmodule LogflareWeb.StripeController do
  use LogflareWeb, :controller

  require Logger

  def event(
        conn,
        %{"id" => id, "type" => type, "data" => %{"object" => %{"customer" => customer} = object}} =
          event
      ) do
    LogflareLogger.context(%{
      billing: %{
        event_id: id,
        customer: customer,
        webhook_type: type
      }
    })

    case type do
      "invoice." <> _sub_type ->
        with %BillingAccount{} = ba <-
               BillingAccounts.get_billing_account_by(stripe_customer: customer),
             {:ok, _billing_account} <- BillingAccounts.sync_invoices(ba) do
          ok(conn)
        else
          nil ->
            customer_not_found(conn)

          err ->
            log_error(err)

            conflict(conn)
        end

      # Lifetime plan deprecated as of October 1, 2020
      "charge.succeeded" ->
        if lifetime_plan_charge?(event) do
          with %BillingAccount{} = ba <-
                 BillingAccounts.get_billing_account_by(stripe_customer: customer),
               {:ok, _ba} <-
                 BillingAccounts.update_billing_account(ba, %{
                   lifetime_plan: true,
                   lifetime_plan_invoice: object["receipt_url"]
                 }) do
            Logger.info("Lifetime customer created. Event id: #{event["id"]}")

            ok(conn)
          else
            nil ->
              customer_not_found(conn)

            err ->
              log_error(err)

              conflict(conn)
          end
        else
          not_implimented(conn)
        end

      "customer.subscription" <> _sub_type ->
        with %BillingAccount{} = ba <-
               BillingAccounts.get_billing_account_by(stripe_customer: customer),
             {:ok, _ba} <- BillingAccounts.sync_subscriptions(ba) do
          Logger.info("Stripe webhook: #{type}")

          ok(conn)
        else
          nil ->
            customer_not_found(conn)

          err ->
            log_error(err)

            conflict(conn)
        end

      _else ->
        not_implimented(conn)
    end
  end

  def event(conn, _params) do
    not_implimented(conn)
  end

  defp ok(conn, message \\ "ok") do
    conn
    |> json(%{message: message})
  end

  defp conflict(conn, message \\ "conflict") do
    conn
    |> put_status(202)
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

  defp customer_not_found(conn) do
    customer = LogflareLogger.context().billing.customer
    type = LogflareLogger.context().billing.webhook_type
    message = "customer not found: #{customer}"

    Logger.warn("Stripe webhook: #{type}")

    ok(conn, message)
  end

  defp log_error(err) do
    type = LogflareLogger.context().billing.webhook_type
    billing = Map.put(LogflareLogger.context().billing, :error_string, inspect(err))

    Logger.error("Stripe webhook error: #{type}", %{billing: billing})
  end
end
