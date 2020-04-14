defmodule LogflareWeb.StripeController do
  use LogflareWeb, :controller

  alias Logflare.Billing

  def event(conn, %{"type" => type, "data" => %{"object" => %{"customer" => customer}}}) do
    case type do
      "invoice." <> _sub_type ->
        {:ok, _billing_account} =
          Billing.get_billing_account_by(stripe_customer: customer)
          |> Billing.sync_invoices()

        conn
        |> json(%{message: "ok"})

      "customer.subscription." <> _sub_type ->
        {:ok, _billing_account} =
          Billing.get_billing_account_by(stripe_customer: customer)
          |> Billing.sync_subscriptions()

        conn
        |> json(%{message: "ok"})

      _else ->
        error(conn)
    end
  end

  def event(conn, _params) do
    error(conn)
  end

  defp error(conn, message \\ "event type not implimented") do
    conn
    |> put_status(202)
    |> json(%{message: message})
  end
end
