defmodule LogflareWeb.BillingAccountLive.Edit do
  @moduledoc """
  Billing Account Edit Page.
  """
  use LogflareWeb, :live_view

  import LogflareWeb.Helpers.Forms
  import Logflare.Sources, only: [count_for_billing: 1]

  alias Logflare.{Users, Billing}

  on_mount {LogflareWeb.AuthLive, :default}

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      Phoenix.PubSub.subscribe(Logflare.PubSub, "billing")
    end

    user =
      socket.assigns.user
      |> Users.preload_sources()
      |> Users.preload_billing_account()

    case user.billing_account do
      nil ->
        socket =
          socket
          |> put_flash(:error, "Create a billing account first!")
          |> redirect(to: ~p"/account/edit#create-a-billing-account")

        {:ok, socket}

      _billing_account ->
        plan = Billing.get_plan_by_user(user)

        socket =
          socket
          |> assign(:period, "month")
          |> assign(:plans, Billing.list_plans())
          |> assign(:plan, plan)
          |> assign(:user, user)
          |> assign(:payment_methods, [])

        {:ok, socket}
    end
  end

  @impl true
  def handle_params(_params, _uri, socket) do
    {:noreply, socket}
  end

  @impl true
  def handle_info({:update_payment_methods, callback, method}, socket) do
    send_update(
      LogflareWeb.BillingAccountLive.PaymentMethodComponent,
      id: "payment_method",
      callback: callback,
      payment_methods: method
    )

    {:noreply, socket}
  end

  def handle_info({:update_billing_account, ba}, socket) do
    send_update(
      LogflareWeb.BillingAccountLive.PaymentMethodComponent,
      id: "payment_method",
      billing_account: ba
    )

    {:noreply, socket}
  end
end
