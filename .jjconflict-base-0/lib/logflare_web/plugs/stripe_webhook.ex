defmodule LogflareWeb.Plugs.StripeWebhook do
  @moduledoc """
  Wraps `Stripe.WebhookPlug` to add logging on rejected webhook requests.

  Logs a warning with diagnostic context whenever a request to `/webhooks/stripe`
  is rejected with a 400, covering all failure cases: missing secret, wrong or
  rotated secret, missing signature header, and forged requests.
  """
  import Plug.Conn
  require Logger

  @behaviour Plug

  def init(opts), do: Stripe.WebhookPlug.init(opts)

  def call(conn, opts) do
    conn
    |> Stripe.WebhookPlug.call(opts)
    |> log_rejections()
  end

  @spec log_rejections(Plug.Conn.t()) :: Plug.Conn.t()
  defp log_rejections(%{status: 400} = conn) do
    Logger.warning("Stripe webhook request rejected", %{
      stripe_signature_present: get_req_header(conn, "stripe-signature") != [],
      stripe_webhook_secret_configured: !!Application.get_env(:logflare, :stripe_webhook_secret)
    })

    conn
  end

  defp log_rejections(result), do: result
end
