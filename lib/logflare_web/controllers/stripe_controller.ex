defmodule LogflareWeb.StripeController do
  use LogflareWeb, :controller

  alias Logflare.Billing

  def event(conn, %{"type" => type, "data" => %{"object" => %{"customer" => customer}}}) do
    case type do
      "invoice." <> _sub_type ->
        sync_and_respond(conn)

      "customer.subscription." <> _sub_type ->
        sync_and_respond(conn)

      _else ->
        error(conn)
    end
  end

  def event(conn, _params) do
    error(conn)
  end

  defp sync_and_respond(conn, message \\ "ok") do
    billing_account = Billing.get_billing_account_by(stripe_customer: customer)
    {:ok, _billing_account} = Billing.sync_billing_account(billing_account)

    conn
    |> json(%{message: message})
  end

  defp error(conn, message \\ "event type not implimented") do
    conn
    |> put_status(202)
    |> json(%{message: message})
  end
end
