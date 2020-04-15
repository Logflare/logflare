defmodule LogflareWeb.StripeController do
  use LogflareWeb, :controller

  require Logger

  alias Logflare.Billing

  def event(conn, %{"type" => type, "data" => %{"object" => %{"customer" => customer}}}) do
    case type do
      "invoice." <> _sub_type ->
        with billing_account <- Billing.get_billing_account_by(stripe_customer: customer),
             {:ok, _billing_account} <- Billing.sync_invoices(billing_account) do
          ok(conn)
        else
          err ->
            Logger.error("Stripe webhook error: #{type}", %{
              billing: %{webhook_type: type, error_string: inspect(err)}
            })

            conflict(conn)
        end

      "customer.subscription." <> _sub_type ->
        with billing_account <- Billing.get_billing_account_by(stripe_customer: customer),
             {:ok, _billing_account} <- Billing.sync_subscriptions(billing_account) do
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
