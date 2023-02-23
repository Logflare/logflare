defmodule LogflareWeb.BillingAccountLive do
  @moduledoc """
  Billing edit LiveView
  """
  require Logger
  use LogflareWeb, :live_view

  alias LogflareWeb.BillingAccountView
  alias Logflare.{Users, Billing}
  alias LogflareWeb.Router.Helpers, as: Routes

  @impl true
  def mount(_params, %{"user_id" => user_id}, socket) do
    if connected?(socket) do
      Phoenix.PubSub.subscribe(Logflare.PubSub, "billing")
    end

    user =
      Users.get(user_id)
      |> Users.preload_sources()
      |> Users.preload_billing_account()

    case user.billing_account do
      nil ->
        socket =
          socket
          |> put_flash(:error, "Create a billing account first!")
          |> redirect(to: Routes.user_path(socket, :edit) <> "#create-a-billing-account")

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
  def handle_event("usage_picker", %{"usage" => %{"days" => days}}, socket)
      when is_binary(days) do
    user = socket.assigns.user

    send_update(LogflareWeb.BillingAccountLive.ChartComponent,
      id: :chart,
      user: user,
      days: String.to_integer(days)
    )

    {:noreply, socket}
  end

  def handle_info({_ref, {:ok, data}}, socket) do
    send_update(LogflareWeb.BillingAccountLive.ChartComponent,
      id: :chart,
      chart_data: data
    )

    {:noreply, socket}
  end

  def handle_info({:DOWN, _ref, _type, _pid, :normal}, socket) do
    # Handle down messages from chart query Task
    {:noreply, socket}
  end

  def handle_info({:chart_tick, counter}, socket) do
    send_update(LogflareWeb.BillingAccountLive.ChartComponent, id: :chart, counter: counter)

    {:noreply, socket}
  end

  def handle_info({:update_payment_methods, callback, method}, socket) do
    send_update(
      LogflareWeb.BillingAccountLive.PaymentMethodComponent,
      id: :payment_method,
      callback: callback,
      payment_methods: method
    )

    {:noreply, socket}
  end

  def handle_info({:update_billing_account, ba}, socket) do
    send_update(
      LogflareWeb.BillingAccountLive.PaymentMethodComponent,
      id: :payment_method,
      billing_account: ba
    )

    {:noreply, socket}
  end

  @impl true
  def render(assigns) do
    BillingAccountView.render("edit.html", assigns)
  end
end
