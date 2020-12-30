defmodule LogflareWeb.BillingAccountLive do
  @moduledoc """
  Billing edit LiveView
  """
  require Logger
  use LogflareWeb, :live_view

  alias LogflareWeb.BillingAccountView
  alias Logflare.{Sources, Users, Plans}

  @impl true
  def render(assigns) do
    IO.inspect(assigns)
    BillingAccountView.render("edit.html", assigns)
  end

  @impl true
  def mount(_params, %{"user_id" => user_id}, socket) do
    user =
      Users.get(user_id)
      |> Users.preload_sources()
      |> Users.preload_billing_account()

    plan = Plans.get_plan_by_user(user)

    socket =
      socket
      |> assign(:period, "month")
      |> assign(:plans, Plans.list_plans())
      |> assign(:plan, plan)
      |> assign(:user, user)

    {:ok, socket}
  end

  @impl true
  def handle_params(_params, _uri, socket) do
    {:noreply, socket}
  end
end
