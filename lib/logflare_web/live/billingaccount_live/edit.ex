defmodule LogflareWeb.BillingAccountLive do
  @moduledoc """
  Billing edit LiveView
  """
  require Logger
  use LogflareWeb, :live_view

  alias LogflareWeb.BillingAccountView
  alias Logflare.{Users, Plans}
  alias LogflareWeb.Router.Helpers, as: Routes

  @impl true
  def mount(_params, %{"user_id" => user_id}, socket) do
    user =
      Users.get(user_id)
      |> Users.preload_sources()
      |> Users.preload_billing_account()

    case user.billing_account do
      nil ->
        socket =
          socket
          |> put_flash(
            :error,
            "Create a billing account first!"
          )
          |> redirect(to: Routes.user_path(socket, :edit) <> "#create-a-billing-account")

        {:ok, socket}

      _billing_account ->
        plan = Plans.get_plan_by_user(user)

        socket =
          socket
          |> assign(:period, "month")
          |> assign(:plans, Plans.list_plans())
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

  def handle_info({:chart_tick, counter}, socket) do
    send_update(LogflareWeb.BillingAccountLive.ChartComponent, id: :chart, counter: counter)

    {:noreply, socket}
  end

  @impl true
  def handle_event("save-payment-and-subscribe", params, socket) do
    send_update(LogflareWeb.BillingAccountLive.PaymentMethodComponent,
      id: :payment_method,
      params: params
    )

    {:noreply, socket}
  end

  @impl true
  def handle_event("clear-flash", _params, socket) do
    socket = clear_flash(socket, :error)

    {:noreply, socket}
  end

  @impl true
  def handle_event("payment-method-error", %{"message" => message}, socket) do
    socket = put_flash(socket, :error, message)

    {:noreply, socket}
  end

  @impl true
  def handle_event("subscribe", _params, socket) do
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

  @impl true
  def render(assigns) do
    BillingAccountView.render("edit.html", assigns)
  end
end
