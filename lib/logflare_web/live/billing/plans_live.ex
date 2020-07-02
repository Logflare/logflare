defmodule LogflareWeb.BillingPlansLive do
  @moduledoc false
  use Phoenix.LiveView, layout: {LogflareWeb.SharedView, "live_widget.html"}
  use Phoenix.HTML

  alias Logflare.Plans
  alias Logflare.Users

  alias LogflareWeb.Router.Helpers, as: Routes

  def mount(_params, %{"user_id" => user_id}, socket) do
    user =
      Users.get(user_id)
      |> Users.preload_sources()
      |> Users.preload_billing_account()

    plan = Plans.get_plan_by_user(user) |> IO.inspect()

    socket =
      socket
      |> assign(:period, "month")
      |> assign(:plans, Plans.list_plans())
      |> assign(:plan, plan)
      |> assign(:user, user)

    {:ok, socket}
  end

  def handle_event("switch_period", %{"period" => period}, socket) do
    {:noreply, assign(socket, :period, period)}
  end

  def render(assigns) do
    ~L"""
    <h5 class="header-margin"><%= String.capitalize(@period) %>ly Pricing</h5
    <p>Get 2 months free if you choose a yearly plan.</p>
    <button phx-click="switch_period" phx-value-period=<%= period!(@period) %> class="btn btn-primary">See pricing per <%= period!(@period) %></button>
    <div class="scrolling-wrapper">
    <div class="container mt-5 min-pricing-width">
    <div class="pricing card-deck row mb-3">
      <div class="card card-pricing text-center px-3 mb-4">
        <span class="h6 w-60 mx-auto px-4 py-1 rounded-bottom bg-primary text-dark shadow-sm">Free</span>
        <div class="bg-transparent card-header pt-4 border-0">
          <h1 class="h1 font-weight-normal text-center mb-0">$<span class="price">0</span></h1>
          <span class="h6 text-muted ml-2">/ per <%= @period %></span> <br>
          <span class="h6 text-muted ml-2">/ per source</span>
        </div>
        <div class="card-body pt-0">
          <ul class="list-unstyled mb-4">
            <li>Alerts every 4 hours</li>
            <li>No custom backend</li>
            <li>No Data Studio integration</li>
            <li>Rate limit 5/second</li>
            <li>300 burst per second</li>
            <li>12,960,000 events/month</li>
            <li>Up to 3 saved searches</li>
            <li>3 day event history</li>
            <li>0 additional users</li>
            <li>Up to 50 fields</li>
          </ul>
        </div>
      </div>
      <div class="card card-pricing text-center px-3 mb-4">
        <span class="h6 w-60 mx-auto px-4 py-1 rounded-bottom bg-primary text-dark shadow-sm">Hobby</span>
        <div class="bg-transparent card-header pt-4 border-0">
          <h1 class="h1 font-weight-normal text-center mb-0"><span class="price"><%= Plans.find_plan(@plans, @period, "Hobby").price |> Money.new(:USD) |> Money.to_string(fractional_unit: false) %></span></h1>
          <span class="h6 text-muted ml-2">/ per <%= @period %></span> <br>
          <span class="h6 text-muted ml-2">/ per source</span>
        </div>
        <div class="card-body pt-0">
          <ul class="list-unstyled mb-4">
            <li>Alerts every 1 hour</li>
            <li>BYOB BigQuery</li>
            <li>Data Studio integration</li>
            <li>Unlimited events (fair use)</li>
            <li>Up to 8 saved searches</li>
            <li>7 day event history</li>
            <li>1 additional user</li>
            <li>Up to 100 fields</li>
          </ul>
          <%= sub_button(@plan, @socket, @plans, @period, "Hobby") %>
        </div>
      </div>
      <div class="card card-pricing text-center px-3 mb-4">
        <span class="h6 w-60 mx-auto px-4 py-1 rounded-bottom bg-primary text-dark shadow-sm">Professional</span>
        <div class="bg-transparent card-header pt-4 border-0">
          <h1 class="h1 font-weight-normal text-center mb-0"><span class="price"><%= Plans.find_plan(@plans, @period, "Pro").price |> Money.new(:USD) |> Money.to_string(fractional_unit: false) %></span></h1>
          <span class="h6 text-muted ml-2">/ per <%= @period %></span> <br>
          <span class="h6 text-muted ml-2">/ per source</span>
        </div>
        <div class="card-body pt-0">
            <ul class="list-unstyled mb-4">
            <li>Alerts every 15 minutes</li>
            <li>BYOB BigQuery</li>
            <li>Data Studio integration</li>
            <li>Unlimited events (fair use)</li>
            <li>Up to 15 saved searches</li>
            <li>30 day event history</li>
            <li>4 additional users</li>
            <li>Up to 250 fields</li>
          </ul>
          <%= sub_button(@plan, @socket, @plans, @period, "Pro") %>
        </div>
      </div>
      <div class="card card-pricing text-center px-3 mb-4">
        <span class="h6 w-60 mx-auto px-4 py-1 rounded-bottom bg-primary text-dark shadow-sm">Business</span>
        <div class="bg-transparent card-header pt-4 border-0">
          <h1 class="h1 font-weight-normal text-center mb-0"><span class="h6 text-muted ml-2"></span><span class="price"><%= Plans.find_plan(@plans, @period, "Business").price |> Money.new(:USD) |> Money.to_string(fractional_unit: false) %></span></h1>
          <span class="h6 text-muted ml-2">/ per <%= @period %></span> <br>
          <span class="h6 text-muted ml-2">/ per source</span>
        </div>
        <div class="card-body pt-0">
          <ul class="list-unstyled mb-4">
            <li>Alerts every 1 minute</li>
            <li>BYOB BigQuery</li>
            <li>Data Studio integration</li>
            <li>Unlimited events (fair use)</li>
            <li>Up to 30 saved searches</li>
            <li>60 day event history</li>
            <li>9 additional users</li>
            <li>Up to 500 fields</li>
          </ul>
          <%= sub_button(@plan, @socket, @plans, @period, "Business") %>
        </div>
      </div>
      <div class="card card-pricing text-center px-3 mb-4">
        <span class="h6 w-60 mx-auto px-4 py-1 rounded-bottom bg-primary text-dark shadow-sm">Enterprise</span>
        <div class="bg-transparent card-header pt-4 border-0">
          <h1 class="h1 font-weight-normal text-center mb-0"><span class="h6 text-muted ml-2"></span><span class="price"><%= 2000 |> Money.new(:USD) |> Money.to_string(fractional_unit: false) %></span></h1>
          <span class="h6 text-muted ml-2">/ starting per <%= @period %></span> <br>
          <span class="h6 text-muted ml-2">/ per source</span>
        </div>
        <div class="card-body pt-0">
          <ul class="list-unstyled mb-4">
            <li>Alerts every 1 second</li>
            <li>BYOB BigQuery</li>
            <li>Any BI integration</li>
            <li>Unlimited events (fair use)</li>
            <li>Up to 100 saved searches</li>
            <li>Unlimited event history</li>
            <li>Unlimited additional users</li>
            <li>Up to 10,000 fields</li>
          </ul>
          <%= sub_button(@plan, @socket, @plans, @period, "Enterprise") %>
          <br>
          <%= link "Contact us", to: Routes.contact_path(@socket, :contact), class: "btn btn-primary form-button" %>
        </div>
      </div>
    </div>
    </div>
    </div>
    """
  end

  def sub_button(plan, socket, plans, period, plan_name) do
    cond do
      is_nil(plan) ->
        link("Subscribe",
          to:
            Routes.billing_path(socket, :confirm_subscription, %{
              "stripe_id" => Plans.find_plan(plans, period, plan_name).stripe_id
            }),
          class: "btn btn-primary form-button"
        )

      plan.id == Plans.find_plan(plans, period, plan_name).id ->
        link("Subscribe",
          to:
            Routes.billing_path(socket, :confirm_subscription, %{
              "stripe_id" => Plans.find_plan(plans, period, plan_name).stripe_id
            }),
          class: "btn btn-primary form-button disabled"
        )

      plan.name == Plans.find_plan(plans, period, plan_name).name ->
        link("Switch to #{period}ly",
          to:
            Routes.billing_path(socket, :change_subscription, %{
              "plan" => Plans.find_plan(plans, period, plan_name).id
            }),
          class: "btn btn-primary form-button"
        )

      plan.id > Plans.find_plan(plans, period, plan_name).id ->
        link("Downgrade",
          to:
            Routes.billing_path(socket, :change_subscription, %{
              "plan" => Plans.find_plan(plans, period, plan_name).id
            }),
          class: "btn btn-primary form-button"
        )

      plan.id < Plans.find_plan(plans, period, plan_name).id ->
        link("Upgrade",
          to:
            Routes.billing_path(socket, :change_subscription, %{
              "plan" => Plans.find_plan(plans, period, plan_name).id
            }),
          class: "btn btn-primary form-button"
        )
    end
  end

  defp period!("month"), do: "year"
  defp period!("year"), do: "month"
end
