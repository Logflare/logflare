defmodule LogflareWeb.StripeWebhookHandler do
  @behaviour Stripe.WebhookHandler

  require Logger

  import Logflare.Utils.Guards

  alias Logflare.Billing
  alias Logflare.Billing.BillingAccount
  alias Logflare.Billing.PaymentMethod

  @impl Stripe.WebhookHandler
  def handle_event(%Stripe.Event{id: id, type: type, data: data}) do
    object = Map.get(data, :object)
    prev_attrs = Map.get(data, :previous_attributes)

    customer = extract_customer(object, prev_attrs)

    if is_binary(customer) do
      LogflareLogger.context(%{
        billing: %{event_id: id, customer: customer, webhook_type: type}
      })

      dispatch(type, object, customer)
    else
      :ok
    end
  end

  @spec dispatch(String.t(), map(), String.t()) :: :ok | {:error, String.t()}
  defp dispatch("invoice." <> _sub_type, _object, customer) do
    with %BillingAccount{} = ba <- Billing.get_billing_account_by(stripe_customer: customer),
         {:ok, _} <- Billing.sync_invoices(ba) do
      :ok
    else
      nil -> log_customer_not_found()
      err -> log_and_error(err)
    end
  end

  defp dispatch("charge.succeeded", %{receipt_url: receipt_url} = object, customer) do
    if lifetime_plan_charge?(object) do
      with %BillingAccount{} = ba <- Billing.get_billing_account_by(stripe_customer: customer),
           {:ok, _} <-
             Billing.update_billing_account(ba, %{
               lifetime_plan: true,
               lifetime_plan_invoice: receipt_url
             }) do
        Logger.info("Lifetime customer created. Customer: #{customer}")
        :ok
      else
        nil -> log_customer_not_found()
        err -> log_and_error(err)
      end
    else
      :ok
    end
  end

  defp dispatch("customer.subscription" <> _sub_type, _object, customer) do
    type = LogflareLogger.context().billing.webhook_type

    with %BillingAccount{} = ba <- Billing.get_billing_account_by(stripe_customer: customer),
         {:ok, _} <- Billing.sync_subscriptions(ba) do
      Logger.info("Stripe webhook: #{type}")
      :ok
    else
      nil -> log_customer_not_found()
      err -> log_and_error(err)
    end
  end

  defp dispatch(
         "payment_method.attached",
         %{
           id: stripe_id,
           card: %{brand: brand, exp_month: exp_month, exp_year: exp_year, last4: last_four}
         },
         customer
       ) do
    params = %{
      customer_id: customer,
      brand: brand,
      exp_month: exp_month,
      exp_year: exp_year,
      last_four: last_four,
      stripe_id: stripe_id
    }

    with nil <- Billing.get_payment_method_by(stripe_id: stripe_id),
         {:ok, pm} <- Billing.create_payment_method(params) do
      Phoenix.PubSub.broadcast(
        Logflare.PubSub,
        "billing",
        {:update_payment_methods, "attached", pm}
      )

      :ok
    else
      %PaymentMethod{} -> :ok
      err -> log_and_error(err)
    end
  end

  defp dispatch("payment_method.detached", %{id: stripe_id}, _customer) do
    with %PaymentMethod{} = pm <- Billing.get_payment_method_by(stripe_id: stripe_id),
         {:ok, _} <- Billing.delete_payment_method(pm) do
      Phoenix.PubSub.broadcast(
        Logflare.PubSub,
        "billing",
        {:update_payment_methods, "detached", %PaymentMethod{stripe_id: stripe_id}}
      )

      :ok
    else
      nil -> :ok
      err -> log_and_error(err)
    end
  end

  defp dispatch(_type, _object, _customer), do: :ok

  @spec extract_customer(map() | nil, map() | nil) :: String.t() | nil
  defp extract_customer(object, prev_attrs) do
    customer_from_object = object && Map.get(object, :customer)
    customer_from_prev = prev_attrs && get_in(prev_attrs, [:customer])

    cond do
      is_non_empty_binary(customer_from_object) -> customer_from_object
      is_non_empty_binary(customer_from_prev) -> customer_from_prev
      true -> nil
    end
  end

  @spec lifetime_plan_charge?(map()) :: boolean()
  defp lifetime_plan_charge?(%{amount: amount}), do: amount == 50_000
  defp lifetime_plan_charge?(_), do: false

  defp log_customer_not_found do
    billing = LogflareLogger.context().billing

    Logger.warning(
      "Stripe webhook: #{billing.webhook_type} - customer not found: #{billing.customer}"
    )

    :ok
  end

  defp log_and_error(err) do
    billing = LogflareLogger.context().billing
    billing_with_err = Map.put(billing, :error_string, inspect(err))
    Logger.error("Stripe webhook error: #{billing.webhook_type}", %{billing: billing_with_err})
    {:error, "processing error"}
  end
end
