defmodule LogflareWeb.BillingPlansLive do
  @moduledoc false
  use Phoenix.LiveView, layout: {LogflareWeb.SharedView, "live_widget.html"}
  use Phoenix.HTML

  alias Logflare.Plans

  alias LogflareWeb.Router.Helpers, as: Routes

  def mount(_params, _session, socket) do
    socket =
      socket
      |> assign(:period, "month")
      |> assign(:plans, Plans.list_plans())

    {:ok, socket}
  end

  def handle_event("switch_period", %{"period" => period}, socket) do
    {:noreply, assign(socket, :period, period)}
  end

  def render(assigns) do
    ~L"""
    <p>Showing pricing per <%= @period %>.</p>
    <button phx-click="switch_period" phx-value-period=<%= period!(@period) %> class="btn btn-primary">See pricing per <%= period!(@period) %></button>
    <div class="scrolling-wrapper">
    <div class="container mt-5 min-pricing-width">
    <div class="pricing card-deck flex-md-row mb-3">
      <div class="card card-pricing text-center px-3 mb-4">
        <span class="h6 w-60 mx-auto px-4 py-1 rounded-bottom bg-primary text-dark shadow-sm">Free</span>
        <div class="bg-transparent card-header pt-4 border-0">
          <h1 class="h1 font-weight-normal text-center mb-0">$<span class="price">0</span></h1>
          <span class="h6 text-muted ml-2">/ per <%= @period %></span>
        </div>
        <div class="card-body pt-0">
          <ul class="list-unstyled mb-4">
            <li>Alerts every 1 hour</li>
            <li>No custom backend</li>
            <li>Rate limit 2/second</li>
            <li>120 burst per second</li>
            <li>5,200,000 events/month</li>
            <li>Up to 3 sources</li>
            <li>7 day event history</li>
            <li>0 additional users</li>
          </ul>
        </div>
      </div>
      <div class="card card-pricing text-center px-3 mb-4">
        <span class="h6 w-60 mx-auto px-4 py-1 rounded-bottom bg-primary text-dark shadow-sm">Entry</span>
        <div class="bg-transparent card-header pt-4 border-0">
          <h1 class="h1 font-weight-normal text-center mb-0"><span class="price"><%= find_plan(@plans, @period, "Entry").price |> Money.new(:USD) |> Money.to_string(fractional_unit: false) %></span></h1>
          <span class="h6 text-muted ml-2">/ per <%= @period %></span>
        </div>
        <div class="card-body pt-0">
          <ul class="list-unstyled mb-4">
            <li>Alerts every 15 minutes</li>
            <li>BYOB BigQuery</li>
            <li>Rate limit 5/second</li>
            <li>300 burst per second</li>
            <li>13,000,000 events/month</li>
            <li>Up to 5 sources</li>
            <li>Unlimited*</li>
            <li>2 additional users</li>
          </ul>
          <%= link "Subscribe", to: Routes.billing_path(@socket, :confirm_subscription, %{"stripe_id" => find_plan(@plans, @period, "Entry").stripe_id}), class: "btn btn-primary form-button" %>
        </div>
      </div>
      <div class="card card-pricing text-center px-3 mb-4">
        <span class="h6 w-60 mx-auto px-4 py-1 rounded-bottom bg-primary text-dark shadow-sm">Professional</span>
        <div class="bg-transparent card-header pt-4 border-0">
          <h1 class="h1 font-weight-normal text-center mb-0"><span class="price"><%= find_plan(@plans, @period, "Pro").price |> Money.new(:USD) |> Money.to_string(fractional_unit: false) %></span></h1>
          <span class="h6 text-muted ml-2">/ per <%= @period %></span>
        </div>
        <div class="card-body pt-0">
          <ul class="list-unstyled mb-4">
            <li>Alerts every 5 minutes</li>
            <li>BYOB BigQuery</li>
            <li>Rate limit 19/second</li>
            <li>1,140 burst per second</li>
            <li>50,000,000 events/month</li>
            <li>Up to 20 sources</li>
            <li>Unlimited*</li>
            <li>20 additional users</li>
          </ul>
          <%= link "Subscribe", to: Routes.billing_path(@socket, :confirm_subscription, %{"stripe_id" => find_plan(@plans, @period, "Pro").stripe_id}), class: "btn btn-primary form-button" %>
        </div>
      </div>
      <div class="card card-pricing text-center px-3 mb-4">
        <span class="h6 w-60 mx-auto px-4 py-1 rounded-bottom bg-primary text-dark shadow-sm">Enterprise</span>
        <div class="bg-transparent card-header pt-4 border-0">
          <h1 class="h1 font-weight-normal text-center mb-0"><span class="h6 text-muted ml-2"></span><span class="price"><%= find_plan(@plans, @period, "Enterprise").price |> Money.new(:USD) |> Money.to_string(fractional_unit: false) %></span></h1>
          <span class="h6 text-muted ml-2">/ starting per <%= @period %></span>
        </div>
        <div class="card-body pt-0">
          <ul class="list-unstyled mb-4">
            <li>Alerts every 1 second</li>
            <li>Custom</li>
            <li>Unlimited</li>
            <li>Unlimited</li>
            <li>1 billion+ events/month</li>
            <li>100+</li>
            <li>Unlimited</li>
            <li>Unlimited</li>
          </ul>
          <%= link "Contact us", to: Routes.contact_path(@socket, :contact), class: "btn btn-primary form-button" %>
        </div>
      </div>
    </div>
    </div>
    </div>
    """
  end

  defp period!("month"), do: :year
  defp period!("year"), do: :month

  defp find_plan(plans, period, name) do
    Enum.filter(plans, fn x -> x.period == period end)
    |> Enum.find(fn x -> x.name == name end)
  end
end
