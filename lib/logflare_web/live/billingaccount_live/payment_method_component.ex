defmodule LogflareWeb.BillingAccountLive.PaymentMethodComponent do
  @moduledoc """
  Billing edit LiveView
  """

  use LogflareWeb, :live_component

  alias Logflare.Billing

  require Logger

  defp env_stripe_publishable_key, do: Application.get_env(:stripity_stripe, :publishable_key)

  def mount(socket) do
    socket = assign(socket, :stripe_key, env_stripe_publishable_key())

    {:ok, socket}
  end

  def update(%{user: user}, socket) do
    payment_methods =
      Billing.list_payment_methods_by(customer_id: user.billing_account.stripe_customer)

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

  def update(%{payment_methods: pm, callback: type}, socket) do
    payment_methods = socket.assigns.payment_methods

    methods =
      case type do
        "attached" ->
          payment_methods ++ [pm]

        "detached" ->
          Enum.reject(payment_methods, fn x -> x.stripe_id == pm.stripe_id end)
      end

    socket =
      socket
      |> assign(:payment_methods, methods)

    {:ok, socket}
  end

  def update(%{billing_account: ba}, socket) do
    user = Map.put(socket.assigns.user, :billing_account, ba)

    socket =
      socket
      |> assign(:user, user)

    {:ok, socket}
  end

  def handle_event("submit", _params, socket) do
    socket = socket |> push_event("submit", %{})
    {:noreply, socket}
  end

  def handle_event("save", params, socket) do
    case Billing.create_payment_method_with_stripe(params) do
      {:ok, m} ->
        methods = socket.assigns.payment_methods ++ [m]

        socket =
          socket
          |> clear_flash()
          |> put_flash(:info, "Payment method created!")
          |> assign(:payment_methods, methods)

        {:noreply, push_patch(socket, to: Routes.billing_account_path(socket, :edit))}

      {:error, _err} ->
        {:noreply, socket}
    end
  end

  def handle_event("delete", %{"id" => id}, socket) do
    with payment_method <-
           Billing.get_payment_method!(id),
         {:ok, _resp} <-
           Billing.delete_payment_method_with_stripe(payment_method) do
      customer = socket.assigns.user.billing_account.stripe_customer
      payment_methods = Billing.list_payment_methods_by(customer_id: customer)

      socket =
        socket
        |> assign(:payment_methods, payment_methods)
        |> clear_flash()
        |> put_flash(:info, "Payment method deleted!")
        |> push_patch(to: Routes.billing_account_path(socket, :edit))

      {:noreply, socket}
    else
      {:error, message} ->
        socket =
          socket
          |> clear_flash()
          |> put_flash(:error, message)
          |> push_patch(to: Routes.billing_account_path(socket, :edit))

        {:noreply, socket}

      _err ->
        socket =
          socket
          |> clear_flash()
          |> put_flash(:error, "Something went wrong. Please contact support if this continues.")
          |> push_patch(to: Routes.billing_account_path(socket, :edit))

        {:noreply, socket}
    end
  end

  def handle_event("sync", _params, socket) do
    customer = socket.assigns.user.billing_account.stripe_customer
    billing_account = socket.assigns.user.billing_account
    user = socket.assigns.user

    with {:ok, payment_methods} <- Billing.sync_payment_methods(customer),
         {:ok, billing_account} <- Billing.sync_billing_account(billing_account) do
      socket =
        socket
        |> assign(:user, Map.put(user, :billing_account, billing_account))
        |> assign(:payment_methods, payment_methods)
        |> clear_flash()
        |> put_flash(:info, "Payment methods successfully synced!")
        |> push_patch(to: Routes.billing_account_path(socket, :edit))

      {:noreply, socket}
    end
  end

  def handle_event("make-default", %{"stripe-id" => id}, socket) do
    billing_account = socket.assigns.user.billing_account
    stripe_customer = billing_account.stripe_customer

    user = socket.assigns.user

    invoice_settings = %{
      invoice_settings: %{
        custom_fields: nil,
        default_payment_method: id,
        footer: nil
      }
    }

    with {:ok, _response} <-
           Billing.Stripe.update_customer(stripe_customer, invoice_settings),
         {:ok, message} <-
           update_all_subscription(billing_account.stripe_subscriptions, %{
             default_payment_method: id
           }),
         {:ok, billing_account} <-
           Billing.update_billing_account(billing_account, %{default_payment_method: id}) do
      socket =
        socket
        |> assign(:user, Map.put(user, :billing_account, billing_account))
        |> clear_flash()
        |> put_flash(:info, message)
        |> push_patch(to: Routes.billing_account_path(socket, :edit))

      {:noreply, socket}
    end
  end

  def render(assigns) do
    ~H"""
    <div>
      <ul class="list-unstyled">
        <%= for p <- @payment_methods do %>
          <li>
            <%= String.upcase(p.brand) %> ending in <%= p.last_four %> expires <%= p.exp_month %>/<%= p.exp_year %> <%= delete_link(
              p,
              @myself
            ) %> <%= if p.stripe_id == @user.billing_account.default_payment_method,
              do: nil,
              else: make_default(p, @myself) %>
          </li>
        <% end %>
      </ul>
      <div id="stripe-elements-form" class="mt-4">
        <form id="payment-form" action="#" phx-submit="submit" phx-hook="PaymentMethodForm" data-stripe-key={@stripe_key} data-stripe-customer={@user.billing_account.stripe_customer} phx-target={@myself}>
          <div id="card-element">
            <!-- Elements will create input elements here -->
          </div>
          <!-- We'll put the error messages in this element -->
          <div id="card-element-errors" role="alert"></div>
          <button type="submit" phx-disable-with="Saving..." class="btn btn-primary form-button mt-4">
            Add payment method
          </button>
        </form>
        <button phx-click="sync" phx-disable-with="Syncing..." phx-target={@myself} class="btn btn-dark btn-sm">
          Sync payment methods
        </button>
      </div>
    </div>
    """
  end

  defp delete_link(p, myself) do
    assigns = %{
      stripe_id: p.id,
      myself: myself
    }

    ~H"""
    <button phx-click="delete" phx-disable-with="Deleting..." phx-value-id={@stripe_id} phx-target={@myself} class="btn btn-danger btn-sm m-3">
      Delete
    </button>
    """
  end

  defp make_default(p, myself) do
    assigns = %{
      stripe_id: p.stripe_id,
      myself: myself
    }

    ~H"""
    <button phx-click="make-default" phx-disable-with="Updating..." phx-value-stripe-id={@stripe_id} phx-target={@myself} class="btn btn-dark btn-sm">
      Make default
    </button>
    """
  end

  defp update_all_subscription(nil, _params),
    do: {:ok, "Default payment method set for your billing account!"}

  defp update_all_subscription(subs, params) do
    updated =
      Enum.count(subs["data"], fn s ->
        match?({:ok, _}, Billing.Stripe.update_subscription(s["id"], params))
      end)

    message = "Default payment method set for #{updated} subscription(s)!"

    {:ok, message}
  end
end
