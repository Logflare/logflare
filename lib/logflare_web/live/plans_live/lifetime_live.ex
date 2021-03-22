defmodule LogflareWeb.LifetimeLive do
  @moduledoc false
  use Phoenix.LiveView, layout: {LogflareWeb.SharedView, "live_widget.html"}
  use Phoenix.HTML

  alias Logflare.Plans
  alias Logflare.Users
  alias Logflare.BillingAccounts

  alias LogflareWeb.Router.Helpers, as: Routes

  def mount(_params, %{"user_id" => user_id}, socket) do
    user =
      Users.get(user_id)
      |> Users.preload_sources()
      |> Users.preload_billing_account()

    plan = Plans.get_plan_by_user(user)
    count = count_billing_accounts()
    left = 100 - count

    socket =
      socket
      |> assign(:period, "life")
      |> assign(:plans, Plans.list_plans())
      |> assign(:plan, plan)
      |> assign(:user, user)
      |> assign(:time, nil)
      |> assign(lifetime_count: count)
      |> assign(lifetime_left: left)

    if connected?(socket) do
      :timer.send_interval(1000, self(), :tick)
    end

    {:ok, socket}
  end

  def mount(_params, _session, socket) do
    count = count_billing_accounts()
    left = 100 - count

    socket =
      socket
      |> assign(:period, "life")
      |> assign(:plans, Plans.list_plans())
      |> assign(:plan, nil)
      |> assign(:user, nil)
      |> assign(:time, nil)
      |> assign(lifetime_count: count)
      |> assign(lifetime_left: left)

    if connected?(socket) do
      :timer.send_interval(:timer.seconds(5), self(), :tick)
    end

    {:ok, socket}
  end

  def handle_info(:tick, session) do
    count = count_billing_accounts()
    left = 100 - count

    session =
      session
      |> assign(lifetime_count: count)
      |> assign(lifetime_left: left)

    {:noreply, session}
  end

  def render(assigns) do
    ~L"""
    <div class="card card-active-popular">
      <div class="card-header text-white border-0 py-4 px-5">
        <h4 class="m-0">Subscribe to Logflare for Life</h4>
      </div>
      <div class="card-body p-5">
        <h5><strong>Get 10 Business level sources for life for a one-time payment of $500.</strong></h5>
        <p><strong><u>Limited availability. Only <%= @lifetime_left %> left!</u></strong></p>
        <div class="progress my-5">
          <div class="progress-bar progress-bar-striped" role="progressbar" style="width: <%= @lifetime_count %>%"
            aria-valuenow="<%= @lifetime_count %>" aria-valuemin="0" aria-valuemax="100"></div>
        </div>
        <p>🤙 We're celebrating the official launch of Logflare v1! For a one time payment of $500 you get access to Logflare
          for life. This deal is limited to the first 100 customers who opt for the Logflare for Life plan.</p>
        <p><strong>Included per source:</strong></p>
        <div class="d-flex flex-wrap">
          <div class="mx-4">
            <p><i class="fas fa-check mr-2"></i>8 saved searches</p>
          </div>
          <div class="mx-4">
            <p><i class="fas fa-check mr-2"></i>1 minute alert frequency</p>
          </div>
          <div class="mx-4">
            <p><i class="fas fa-check mr-2"></i>Google Data Studio or any BI tool for reporting</p>
          </div>
          <div class="mx-4">
            <p><i class="fas fa-check mr-2"></i>BYOB BigQuery tables</p>
          </div>
          <div class="mx-4">
            <p><i class="fas fa-check mr-2"></i>500 fields per source</p>
          </div>
          <div class="mx-4">
            <p><i class="fas fa-check mr-2"></i>60 days of history included</p>
          </div>
          <div class="mx-4">
            <p><i class="fas fa-check mr-2"></i>Unlimited<sup>2</sup> events per month</p>
          </div>
          <div class="mx-4">
            <p><i class="fas fa-check mr-2"></i>Unlimited<sup>2</sup> rate limit</p>
          </div>
          <div class="mx-4">
            <p><i class="fas fa-check mr-2"></i>Unlimited<sup>2</sup> rate burst</p>
          </div>
        </div>
        <p><strong>Included with your account:</strong></p>
        <div class="d-flex flex-wrap">
          <div class="mx-4">
            <p><i class="fas fa-check mr-2"></i>10 sources</p>
          </div>
          <div class="mx-4">
            <p><i class="fas fa-check mr-2"></i>9 additional team members</p>
          </div>
          <div class="mx-4">
            <p><i class="fas fa-check mr-2"></i>14-day money-back guarantee</p>
          </div>
        </div>
        <%= if @user && @user.billing_account do %>
        <%= if @user.billing_account.lifetime_plan do %>
        <%= link("Lifetime plan invoice",
                    to: @user.billing_account.lifetime_plan_invoice,
                    class: "btn btn-light px-4 py-2 my-4"
                  )
                  %>
        <% else %>
        <%= link("Get Logflare for life for only $500",
                  to:
                    Routes.billing_path(@socket, :confirm_subscription, %{
                      "stripe_id" => Plans.find_plan(@plans, @period, "Lifetime").stripe_id,
                      "mode" => "payment"
                    }),
                  class: "btn btn-pink px-4 py-2 my-4"
                )
                %>
        <% end %>

        <% else %>
        <%= link("Get Logflare for life for only $500",
                  to:
                    Routes.billing_path(@socket, :confirm_subscription, %{
                      "stripe_id" => Plans.find_plan(@plans, @period, "Lifetime").stripe_id,
                      "mode" => "payment"
                    }),
                  class: "btn btn-pink px-4 py-2 my-4"
                )
                %>
        <% end %>

      </div>
    </div>
    """
  end

  defp count_billing_accounts() do
    BillingAccounts.list_billing_accounts()
    |> Enum.count(fn ba -> ba.lifetime_plan == true end)
  end
end
