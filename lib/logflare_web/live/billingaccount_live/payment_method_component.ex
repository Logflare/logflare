defmodule LogflareWeb.BillingAccountLive.PaymentMethodComponent do
  @moduledoc """
  Billing edit LiveView
  """

  use LogflareWeb, :live_component
  use Phoenix.HTML

  alias Logflare.PaymentMethods

  require Logger

  @stripe_publishable_key Application.get_env(:stripity_stripe, :publishable_key)

  def preload(assigns) when is_list(assigns) do
    assigns
  end

  def mount(socket) do
    socket = assign(socket, :stripe_key, @stripe_publishable_key)

    {:ok, socket}
  end

  def update(%{user: user}, socket) do
    payment_methods =
      PaymentMethods.list_payment_methods_by(customer_id: user.billing_account.stripe_customer)

    socket =
      case connected?(socket) do
        true ->
          assign(socket, :loading, false)

        false ->
          assign(socket, :loading, true)
      end
      |> assign(:user, user)
      |> assign(:payment_methods, payment_methods)

    {:ok, socket}
  end

  def handle_event(
        "save-payment-method",
        %{"customer_id" => cust_id, "id" => pm_id, "price_id" => price_id},
        socket
      ) do
    case PaymentMethods.create_payment_method(%{
           "customer_id" => cust_id,
           "stripe_id" => pm_id,
           "price_id" => price_id
         }) do
      {:ok, m} ->
        methods = [m | socket.assigns.payment_methods]

        socket =
          socket
          |> put_flash(:info, "Payment method created!")
          |> assign(:payment_methods, methods)

        {:noreply, push_patch(socket, to: Routes.billing_account_path(socket, :edit))}

      {:error, _err} ->
        {:noreply, socket}
    end
  end

  def render(assigns) do
    ~L"""
    <ul>
    <%= for p <- @payment_methods do %>
    <li><%= p.stripe_id %></li>
    <% end %>
    </ul>
    <p>Manage you payment methods.</p>
    <div id="payment-method"  class="my-3 w-auto">
      <div id="stripe-elements-form" class="w-50 mt-4">
        <form id="payment-form" action="#" phx-submit="subscribe" phx-hook="PaymentMethodForm" data-stripe-key="<%= @stripe_key %>" data-stripe-customer="<%= @user.billing_account.stripe_customer %>">
          <div id="card-element">
            <!-- Elements will create input elements here -->
          </div>
          <!-- We'll put the error messages in this element -->
          <div id="card-element-errors" role="alert"></div>
          <button type="submit" phx-disable-with="Saving..." class="btn btn-primary form-button mt-4">Add payment method</button>
        </form>
      </div>

    </div>
    """
  end
end
