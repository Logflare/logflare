defmodule Logflare.BillingAccounts.Stripe do
  @moduledoc """
    Communicate with the Stripe API.
  """
  alias LogflareWeb.Router.Helpers, as: Routes
  alias LogflareWeb.Endpoint
  alias Logflare.BillingAccounts.BillingAccount
  alias Logflare.BillingAccounts
  alias Logflare.Plans.Plan
  alias Logflare.User
  alias Logflare.Sources

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
          sources: sources,
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
      return_url: Routes.billing_url(Endpoint, :edit)
    }

    Stripe.BillingPortal.Session.create(params)
  end

  def change_subscription(billing_account, sources, to_plan) do
    [subscription] = billing_account.stripe_subscriptions["data"]
    {:ok, item} = BillingAccounts.get_billing_account_stripe_subscription_item(billing_account)

    delete_item = %{id: item["id"], deleted: true}
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
end
