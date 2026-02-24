defmodule Logflare.Billing.Stripe do
  @moduledoc """
    Communicate with the Stripe API.
  """
  use LogflareWeb, :routes

  alias LogflareWeb.Endpoint
  alias Logflare.Billing.{BillingAccount, Plan}
  alias Logflare.Billing
  alias Logflare.User
  alias Logflare.Sources

  @trial_days_default 15

  def create_add_credit_card_session(%BillingAccount{} = billing_account) do
    stripe_customer_id = billing_account.stripe_customer
    [subscription] = billing_account.stripe_subscriptions["data"]

    params = %{
      customer: stripe_customer_id,
      mode: "setup",
      payment_method_types: ["card"],
      success_url: Routes.billing_url(Endpoint, :update_credit_card_success),
      cancel_url: Routes.billing_url(Endpoint, :abandoned),
      setup_intent_data: %{metadata: %{subscription_id: subscription["id"]}}
    }

    Stripe.Session.create(params)
  end

  def create_payment_session(
        %User{
          sources: _sources,
          billing_account: %BillingAccount{stripe_customer: stripe_customer_id}
        } = _user,
        %Plan{stripe_id: stripe_id} = _plan
      ) do
    params = %{
      customer: stripe_customer_id,
      mode: "payment",
      payment_method_types: ["card"],
      success_url: Routes.billing_url(Endpoint, :success),
      cancel_url: Routes.billing_url(Endpoint, :abandoned),
      line_items: [
        %{price: stripe_id, quantity: 1}
      ]
    }

    Stripe.Session.create(params)
  end

  def create_customer_session(
        %User{
          sources: sources,
          billing_account: %BillingAccount{stripe_customer: stripe_customer_id}
        } = _user,
        %Plan{stripe_id: stripe_id} = _plan
      ) do
    count = Sources.count_for_billing(sources)

    params = %{
      customer: stripe_customer_id,
      mode: "subscription",
      payment_method_types: ["card"],
      success_url: Routes.billing_url(Endpoint, :success),
      cancel_url: Routes.billing_url(Endpoint, :abandoned),
      line_items: [
        %{price: stripe_id, quantity: count}
      ],
      subscription_data: %{trial_from_plan: true}
    }

    Stripe.Session.create(params)
  end

  def create_metered_customer_session(
        %User{
          sources: _sources,
          billing_account: %BillingAccount{stripe_customer: stripe_customer_id}
        } = _user,
        %Plan{stripe_id: stripe_id} = _plan
      ) do
    params = %{
      customer: stripe_customer_id,
      mode: "subscription",
      payment_method_types: ["card"],
      success_url: Routes.billing_url(Endpoint, :success),
      cancel_url: Routes.billing_url(Endpoint, :abandoned),
      line_items: [
        %{price: stripe_id}
      ],
      subscription_data: %{trial_end: trial_end()}
    }

    Stripe.Session.create(params)
  end

  def find_completed_session(session_id) do
    with {:ok, %Stripe.List{data: events}} <-
           list_stripe_events_by(%{type: "checkout.session.completed"}),
         %Stripe.Event{data: %{object: %Stripe.Session{} = stripe_session}} <-
           Enum.find(events, fn %Stripe.Event{} = e -> e.data.object.id == session_id end) do
      {:ok, stripe_session}
    else
      err -> err
    end
  end

  def create_billing_portal_session(%BillingAccount{} = billing_account) do
    params = %{
      customer: billing_account.stripe_customer,
      return_url: url(~p"/billing/edit")
    }

    Stripe.BillingPortal.Session.create(params)
  end

  def change_subscription(billing_account, sources, to_plan) do
    [subscription] = billing_account.stripe_subscriptions["data"]
    item = Billing.get_billing_account_stripe_subscription_item(billing_account)

    delete_item = %{id: item["id"], deleted: true}
    add_item = %{price: to_plan.stripe_id, quantity: Enum.count(sources)}
    params = %{items: [delete_item, add_item]}

    update_subscription(subscription["id"], params)
  end

  def change_from_metered_subscription(billing_account, _sources, to_plan) do
    [subscription] = billing_account.stripe_subscriptions["data"]
    item = Billing.get_billing_account_stripe_subscription_item(billing_account)

    delete_item = %{id: item["id"], deleted: true, clear_usage: true}
    add_item = %{price: to_plan.stripe_id}
    params = %{items: [delete_item, add_item]}

    update_subscription(subscription["id"], params)
  end

  def change_to_metered_subscription(billing_account, _sources, to_plan) do
    [subscription] = billing_account.stripe_subscriptions["data"]
    item = Billing.get_billing_account_stripe_subscription_item(billing_account)

    delete_item = %{id: item["id"], deleted: true}
    add_item = %{price: to_plan.stripe_id}
    params = %{items: [delete_item, add_item]}

    update_subscription(subscription["id"], params)
  end

  def change_metered_to_standard_subscription(billing_account, sources, to_plan) do
    [subscription] = billing_account.stripe_subscriptions["data"]
    item = Billing.get_billing_account_stripe_subscription_item(billing_account)

    delete_item = %{id: item["id"], deleted: true, clear_usage: true}
    add_item = %{price: to_plan.stripe_id, quantity: Enum.count(sources)}
    params = %{items: [delete_item, add_item]}

    update_subscription(subscription["id"], params)
  end

  def list_customer_invoices(stripe_customer_id) do
    params = %{customer: stripe_customer_id}
    Stripe.Invoice.list(params)
  end

  def get_setup_intent(id) do
    params = %{}
    Stripe.SetupIntent.retrieve(id, params)
  end

  def create_customer(user) do
    params = %{name: user.name, email: user.email}
    Stripe.Customer.create(params)
  end

  def update_customer(id, params) do
    Stripe.Customer.update(id, params)
  end

  def delete_customer(id) do
    Stripe.Customer.delete(id)
  end

  def retrieve_customer(id) do
    Stripe.Customer.retrieve(id)
  end

  def create_subscription(id, pm_id, price_id) do
    items = [%{price: price_id}]

    response =
      with {:ok, _response} <- attatch_payment_method(id, pm_id),
           {:ok, response} <-
             Stripe.Subscription.create(%{
               customer: id,
               default_payment_method: pm_id,
               items: items
             }) do
        {:ok, response}
      else
        err -> err
      end

    response
  end

  def attatch_payment_method(id, pm_id) do
    Stripe.PaymentMethod.attach(%{customer: id, payment_method: pm_id})
  end

  def detach_payment_method(pm_id) do
    Stripe.PaymentMethod.detach(%{payment_method: pm_id})
  end

  def list_payment_methods(id) do
    # TODO: once we upgrade to v3 of :stripity_stripe we should revert the type to atom :card
    Stripe.PaymentMethod.list(%{customer: id, type: "card"})
  end

  def delete_subscription(id) do
    Stripe.Subscription.delete(id)
  end

  def list_customer_subscriptions(stripe_customer_id) do
    params = %{customer: stripe_customer_id}
    Stripe.Subscription.list(params)
  end

  def get_subscription(id) do
    Stripe.Subscription.retrieve(id)
  end

  def update_subscription(id, params) do
    Stripe.Subscription.update(id, params)
  end

  def list_stripe_events_by(params) when is_map(params) do
    Stripe.Event.list(params)
  end

  def get_subscription_item(id, opts \\ []) do
    Stripe.SubscriptionItem.retrieve(id, opts)
  end

  def update_subscription_item(id, params, opts \\ []) do
    Stripe.SubscriptionItem.update(id, params, opts)
  end

  def record_usage(subscription_item_id, usage) do
    timestamp = DateTime.utc_now() |> DateTime.to_unix()

    params = %{
      quantity: usage,
      timestamp: timestamp
    }

    Stripe.SubscriptionItem.Usage.create(subscription_item_id, params)
  end

  def trial_end(days \\ @trial_days_default) do
    DateTime.utc_now()
    |> DateTime.add(:timer.hours(24) * days, :millisecond)
    |> DateTime.to_unix()
  end
end
