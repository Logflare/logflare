defmodule Logflare.Billing.Stripe do
  alias LogflareWeb.Router.Helpers, as: Routes
  alias LogflareWeb.Endpoint

  def create_new_customer_session(email) do
    plan = "plan_Gdg8UxRU0Q0EDH"

    params = %{
      customer_email: email,
      payment_method_types: ["card"],
      # mode: "setup",
      success_url: Routes.billing_url(Endpoint, :success),
      cancel_url: Routes.billing_url(Endpoint, :abandoned),
      subscription_data: %{items: [%{plan: plan}], trial_period_days: 14}
    }

    Stripe.Session.create(params)
  end

  def create_customer_session(stripe_customer_id) do
    plan = "plan_Gdg8UxRU0Q0EDH"

    params = %{
      customer: stripe_customer_id,
      payment_method_types: ["card"],
      success_url: Routes.billing_url(Endpoint, :success),
      cancel_url: Routes.billing_url(Endpoint, :abandoned),
      subscription_data: %{items: [%{plan: plan}], trial_period_days: 14}
    }

    Stripe.Session.create(params)
  end

  def find_completed_session(session_id) do
    {:ok, %Stripe.List{data: events}} =
      list_stripe_events_by(%{type: "checkout.session.completed"})

    event = Enum.find(events, fn %Stripe.Event{} = e -> e.data.object.id == session_id end)

    event.data.object
  end

  def list_stripe_events_by(params) when is_map(params) do
    Stripe.Event.list(params)
  end
end
