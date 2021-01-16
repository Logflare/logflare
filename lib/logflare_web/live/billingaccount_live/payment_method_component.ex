defmodule LogflareWeb.BillingAccountLive.PaymentMethodComponent do
  @moduledoc """
  Billing edit LiveView
  """

  use LogflareWeb, :live_component
  use Phoenix.HTML

  require Logger

  @stripe_publishable_key Application.get_env(:stripity_stripe, :publishable_key)

  def mount(socket) do
    payment_methods = []

    socket =
      assign(socket, :stripe_key, @stripe_publishable_key)
      |> assign(:payment_methods, payment_methods)

    IO.puts("BOOM")

    {:ok, socket}
  end

  def update(%{params: _params} = assigns, socket) do
    IO.puts("updated")

    methods = [assigns | socket.assigns.payment_methods]

    socket =
      socket
      |> assign(:payment_methods, methods)

    {:ok, socket}
  end

  def update(_assigns, socket) do
    IO.puts("updated")

    socket =
      case connected?(socket) do
        true ->
          assign(socket, :loading, false)

        false ->
          assign(socket, :loading, true)
      end

    {:ok, socket}
  end

  def render(assigns) do
    ~L"""
    <div>
    <%= inspect(@payment_methods) %>
    </div>
    <p>Add a new payment method.</p>
    <div id="payment-method-form" phx-hook="PaymentMethodForm" data-stripe-key="<%= @stripe_key %>" class="my-3 w-auto">


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
