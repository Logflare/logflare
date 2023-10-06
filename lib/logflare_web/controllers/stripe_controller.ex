defmodule LogflareWeb.StripeController do
  use LogflareWeb, :controller

  require Logger

  alias Logflare.Billing
  alias Logflare.Billing.BillingAccount
  alias Logflare.Billing.PaymentMethod

  def event(
        conn,
        %{"id" => id, "type" => type, "data" => %{"object" => %{"customer" => customer} = object}} =
          event
      )
      when is_binary(customer) do
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
               Billing.get_billing_account_by(stripe_customer: customer),
             {:ok, _billing_account} <- Billing.sync_invoices(ba) do
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
                 Billing.get_billing_account_by(stripe_customer: customer),
               {:ok, _ba} <-
                 Billing.update_billing_account(ba, %{
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
               Billing.get_billing_account_by(stripe_customer: customer),
             {:ok, _ba} <- Billing.sync_subscriptions(ba) do
          Logger.info("Stripe webhook: #{type}")

          ok(conn)
        else
          nil ->
            customer_not_found(conn)

          err ->
            log_error(err)

            conflict(conn)
        end

      "payment_method.attached" ->
        stripe_id = object["id"]

        params = %{
          customer_id: customer,
          brand: object["card"]["brand"],
          exp_month: object["card"]["exp_month"],
          exp_year: object["card"]["exp_year"],
          last_four: object["card"]["last4"],
          stripe_id: stripe_id
        }

        with nil <- Billing.get_payment_method_by(stripe_id: stripe_id),
             {:ok, pm} <-
               Billing.create_payment_method(params) do
          Phoenix.PubSub.broadcast(
            Logflare.PubSub,
            "billing",
            {:update_payment_methods, "attached", pm}
          )

          ok(conn)
        else
          %PaymentMethod{} ->
            conflict(conn)

          err ->
            log_error(err)

            conflict(conn)
        end

      _else ->
        not_implimented(conn)
    end
  end

  def event(
        conn,
        %{
          "id" => id,
          "type" => type,
          "data" => %{"object" => object, "previous_attributes" => %{"customer" => customer}}
        } = _event
      )
      when is_binary(customer) do
    LogflareLogger.context(%{
      billing: %{
        event_id: id,
        customer: customer,
        webhook_type: type
      }
    })

    case type do
      "payment_method.detached" ->
        stripe_id = object["id"]

        with %PaymentMethod{} = pm <- Billing.get_payment_method_by(stripe_id: stripe_id),
             {:ok, _pm} <-
               Billing.delete_payment_method(pm) do
          Phoenix.PubSub.broadcast(
            Logflare.PubSub,
            "billing",
            {:update_payment_methods, "detached", %PaymentMethod{stripe_id: stripe_id}}
          )

          ok(conn)
        else
          nil ->
            conflict(conn)

          err ->
            log_error(err)

            conflict(conn)
        end

      _else ->
        not_implimented(conn)
    end
  end

  def event(
        conn,
        %{
          "id" => id,
          "type" => type,
          "data" => %{
            "object" => %{"id" => customer, "invoice_settings" => _invoice_settings} = _object
          }
        } = _event
      )
      when is_binary(customer) do
    LogflareLogger.context(%{
      billing: %{
        event_id: id,
        customer: customer,
        webhook_type: type
      }
    })

    not_implimented(conn)
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
    event["data"]["object"]["amount"] == 50_000
  end

  defp customer_not_found(conn) do
    customer = LogflareLogger.context().billing.customer
    type = LogflareLogger.context().billing.webhook_type
    message = "customer not found: #{customer}"

    Logger.warning("Stripe webhook: #{type}")

    ok(conn, message)
  end

  defp log_error(err) do
    type = LogflareLogger.context().billing.webhook_type
    billing = Map.put(LogflareLogger.context().billing, :error_string, inspect(err))

    Logger.error("Stripe webhook error: #{type}", %{billing: billing})
  end
end
