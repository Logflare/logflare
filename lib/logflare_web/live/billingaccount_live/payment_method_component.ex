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

  def handle_event("submit", _params, socket) do
    socket = socket |> push_event("submit", %{})
    {:noreply, socket}
  end

  def handle_event("save", params, socket) do
    case PaymentMethods.create_payment_method_with_stripe(params) do
      {:ok, m} ->
        methods = socket.assigns.payment_methods ++ [m]

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
    <ul class="list-unstyled">
    <%= for p <- @payment_methods do %>
    <li><%= String.upcase(p.brand) %> ending in <%= p.last_four %> expires <%= p.exp_month %>/<%= p.exp_year %> <%= delete_link(p, @myself) %> <%= if p.stripe_id == @user.billing_account.default_payment_method, do: nil, else: make_default(p, @myself) %></li>
    <% end %>
    </ul>
    <div id="stripe-elements-form" class="w-50 mt-4">
      <form id="payment-form" action="#" phx-submit="submit" phx-hook="PaymentMethodForm" data-stripe-key="<%= @stripe_key %>" data-stripe-customer="<%= @user.billing_account.stripe_customer %>" phx-target="<%= @myself %>">
        <div id="card-element">
          <!-- Elements will create input elements here -->
        </div>
        <!-- We'll put the error messages in this element -->
        <div id="card-element-errors" role="alert"></div>
        <button type="submit" phx-disable-with="Saving..." class="btn btn-primary form-button mt-4">Add payment method</button>
      </form>
      <button phx-click="sync" phx-disable-with="Syncing..." phx-target="<%= @myself %>" class="btn btn-dark btn-sm">Sync payment methods</button>
    </div>

    """
  end

  defp delete_link(p, myself) do
    ~E"""
    <button phx-click="delete" phx-disable-with="Deleting..." phx-value-id="<%= p.id %>" phx-target="<%= myself %>" class="btn btn-danger btn-sm m-3">Delete</button>
    """
  end

  defp make_default(p, myself) do
    ~E"""
    <button phx-click="make-default" phx-disable-with="Updating..." phx-value-stripe-id="<%= p.stripe_id %>" phx-target="<%= myself %>" class="btn btn-dark btn-sm">Make default</button>
    """
  end
end
