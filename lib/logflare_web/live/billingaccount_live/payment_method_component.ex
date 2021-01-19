defmodule LogflareWeb.BillingAccountLive.PaymentMethodComponent do
  @moduledoc """
  Billing edit LiveView
  """

  use LogflareWeb, :live_component
  use Phoenix.HTML

  alias Logflare.PaymentMethods
  alias Logflare.BillingAccounts

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
    case PaymentMethods.create_payment_method(cust_id, pm_id, price_id) do
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

  def handle_event("delete", %{"id" => id}, socket) do
    resp =
      PaymentMethods.get_payment_method!(id)
      |> PaymentMethods.delete_payment_method()

    case resp do
      {:ok, _r} ->
        customer = socket.assigns.user.billing_account.stripe_customer
        payment_methods = PaymentMethods.list_payment_methods_by(customer_id: customer)

        socket =
          socket
          |> assign(:payment_methods, payment_methods)
          |> put_flash(:info, "Payment method deleted!")
          |> push_patch(to: Routes.billing_account_path(socket, :edit))

        {:noreply, socket}

      {:error, message} ->
        socket =
          socket
          |> put_flash(:error, message)
          |> push_patch(to: Routes.billing_account_path(socket, :edit))

        {:noreply, socket}
    end
  end

  def handle_event("sync", _params, socket) do
    customer = socket.assigns.user.billing_account.stripe_customer
    {:ok, payment_methods} = PaymentMethods.sync_payment_methods(customer)

    socket =
      socket
      |> assign(:payment_methods, payment_methods)
      |> put_flash(:info, "Payment methods successfully synced!")
      |> push_patch(to: Routes.billing_account_path(socket, :edit))

    {:noreply, socket}
  end

  def handle_event("make-default", %{"stripe-id" => id}, socket) do
    billing_account = socket.assigns.user.billing_account

    {:ok, billing_account} =
      BillingAccounts.update_billing_account(billing_account, %{default_payment_method: id})

    user = socket.assigns.user |> Map.put(:billing_account, billing_account)

    socket =
      socket
      |> assign(:user, user)
      |> put_flash(:info, "Default payment method updated!")
      |> push_patch(to: Routes.billing_account_path(socket, :edit))

    {:noreply, socket}
  end

  def render(assigns) do
    ~L"""
    <ul>
    <%= for p <- @payment_methods do %>
    <li><%= p.stripe_id %> - <%= delete_link(p, @myself) %> - <%= if p.stripe_id == @user.billing_account.default_payment_method, do: nil, else: make_default(p, @myself) %></li>
    <% end %>
    </ul>
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
    <p>WTF</p>
    </div>
    <%= link "Sync payment methods", to: "#", phx_click: "sync", phx_target: @myself, class: "btn btn-primary btn-sm" %>
    """
  end

  defp delete_link(p, myself) do
    ~E"""
    <%= link "delete", to: "#", phx_click: "delete", phx_value_id: p.id, phx_target: myself %>
    """
  end

  defp make_default(p, myself) do
    ~E"""
    <%= link "make default", to: "#", phx_click: "make-default", phx_value_stripe_id: p.stripe_id, phx_target: myself %>
    """
  end
end
