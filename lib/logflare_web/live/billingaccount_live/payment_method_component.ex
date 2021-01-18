defmodule LogflareWeb.BillingAccountLive.PaymentMethodComponent do
  @moduledoc """
  Billing edit LiveView
  """

  use LogflareWeb, :live_component
  use Phoenix.HTML

  alias Logflare.BillingAccounts.Stripe

  require Logger

  @stripe_publishable_key Application.get_env(:stripity_stripe, :publishable_key)

  def preload(assigns) when is_list(assigns) do
    assigns
  end

  def mount(socket) do
    payment_methods = []

    socket =
      assign(socket, :stripe_key, @stripe_publishable_key)
      |> assign(:payment_methods, payment_methods)

    {:ok, socket}
  end

  def update(
        %{
          id: :payment_method,
          params: %{"customer_id" => cust_id, "id" => pm_id, "price_id" => price_id}
        },
        socket
      ) do
    method = Stripe.create_subscription(cust_id, pm_id, price_id)
    methods = [method | socket.assigns.payment_methods]

    socket =
      socket
      |> assign(:payment_methods, methods)

    {:ok, socket}
  end

  def update(%{user: user}, socket) do
    socket =
      case connected?(socket) do
        true ->
          assign(socket, :loading, false)

        false ->
          assign(socket, :loading, true)
      end
      |> assign(:user, user)

    {:ok, socket}
  end

  def render(assigns) do
    ~L"""
    <div>
    <%= inspect(@payment_methods) %>
    </div>
    <p>Add a new payment method.</p>
    <div id="payment-method-form" phx-hook="PaymentMethodForm" data-stripe-key="<%= @stripe_key %>" data-stripe-customer="<%= @user.billing_account.stripe_customer %>" class="my-3 w-auto">


      <div id="stripe-elements-form" class="w-50 mt-4">
        <form id="payment-form" action="#" phx-submit="subscribe">
          <div id="card-element">
            <!-- Elements will create input elements here -->
          </div>
          <!-- We'll put the error messages in this element -->
          <div id="card-element-errors" role="alert"></div>
          <button type="submit" phx-disable-with="Saving..." class="btn btn-primary form-button mt-4">Subscribe</button>
        </form>
      </div>

    </div>
    """
  end
end
