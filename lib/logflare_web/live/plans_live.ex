defmodule LogflareWeb.PlansLive do
  @moduledoc false
  use Phoenix.LiveView, layout: {LogflareWeb.SharedView, :live_widget}
  use Phoenix.HTML

  alias Logflare.Billing
  alias Logflare.Users

  alias LogflareWeb.Router.Helpers, as: Routes

  def render(assigns) do
    ~H"""
    <div class="scrolling-wrapper">
      <div class="min-pricing-width">
        <div class="d-flex my-5 justify-content-center">
          <div class="card py-3 mx-2 bg-transparent">
            <div class="card-header-min-height p-4 border-0"></div>
            <div class="card-body p-0">
              <ul class="list-unstyled">
                <li class="p-2">Team members</li>
                <li class="p-2">
                  Sources
                  <%= link to: Routes.marketing_path(@socket, :pricing) <> "#sources", class: "position-absolute absolute-right" do %>
                    <i class="fas fa-info-circle"></i>
                  <% end %>
                </li>
                <hr />
                <li class="p-2">Alert frequency</li>
                <li class="p-2">
                  Dashboards
                  <%= link to: Routes.marketing_path(@socket, :pricing) <> "#dashboards", class: "position-absolute absolute-right" do %>
                    <i class="fas fa-info-circle"></i>
                  <% end %>
                </li>
                <hr />
                <li class="p-2">
                  Backend
                  <%= link to: Routes.marketing_path(@socket, :pricing) <> "#backend", class: "position-absolute absolute-right" do %>
                    <i class="fas fa-info-circle"></i>
                  <% end %>
                </li>
                <li class="p-2">
                  Fields
                  <%= link to: Routes.marketing_path(@socket, :pricing) <> "#fields", class: "position-absolute absolute-right" do %>
                    <i class="fas fa-info-circle"></i>
                  <% end %>
                </li>
                <hr />
                <li class="p-2">
                  Event Retention
                  <%= link to: Routes.marketing_path(@socket, :pricing) <> "#retention", class: "position-absolute absolute-right" do %>
                    <i class="fas fa-info-circle"></i>
                  <% end %>
                </li>
                <li class="p-2">Events per month</li>
                <hr />
                <li class="p-2">
                  Rate limit
                  <%= link to: Routes.marketing_path(@socket, :pricing) <> "#rate-limit", class: "position-absolute absolute-right" do %>
                    <i class="fas fa-info-circle"></i>
                  <% end %>
                </li>
                <li class="p-2">Rate burst</li>
              </ul>
            </div>
          </div>
          <div class="card card-active-hover text-center py-3 mx-2">
            <div class="card-header-min-height p-4 border-0">
              <h3 class="text-white">Project</h3>
            </div>
            <div class="card-body p-0">
              <ul class="list-unstyled">
                <li class="p-2">0 additional</li>
                <li class="p-2">Unlimited</li>
                <hr />
                <li class="p-2">4 hours</li>
                <li class="p-2">None</li>
                <hr />
                <li class="p-2">Logflare</li>
                <li class="p-2">Up to 50</li>
                <hr />
                <li class="p-2">Up to 3 days</li>
                <li class="p-2">12,960,000</li>
                <hr />
                <li class="p-2">5 per second</li>
                <li class="p-2">300</li>
              </ul>
              <div class="py-4">
                <h2 class="text-white mb-1">$<span class="price">0</span></h2>
                <small class="text-muted">per <%= @period %></small>
              </div>
              <div class="py-4">
                <%= link("Continue", to: Routes.auth_path(@socket, :login), class: "btn btn-dark text-white w-75 mr-0") %>
              </div>
            </div>
          </div>
          <div class="card card-active-hover text-center py-3 mx-2">
            <div class="card-header-min-height p-4 border-0">
              <h3 class="text-white">Metered</h3>
            </div>
            <div class="card-body p-0">
              <ul class="list-unstyled">
                <li class="p-2">Unlimited</li>
                <li class="p-2">Unlimited</li>
                <hr />
                <li class="p-2">1 minute</li>
                <li class="p-2">Google Data Studio</li>
                <hr />
                <li class="p-2">Logflare<sup>1</sup></li>
                <li class="p-2">Up to 500</li>
                <hr />
                <li class="p-2">Up to 90 days</li>
                <li class="p-2">Unlimited</li>
                <hr />
                <li class="p-2">Unlimited</li>
                <li class="p-2">Unlimited</li>
              </ul>
              <div class="py-4">
                <h2 class="text-white">
                  <%= Billing.find_plan(@plans, @period, "Metered").price |> Money.new(:USD) |> Money.to_string(fractional_unit: false) %>
                </h2>
                <small class="text-muted">starts at</small>
                <br />
                <small class="text-muted">per million log events</small>
                <br />
                <small class="text-muted">after one million</small>
                <br />
                <small class="text-muted">paid <%= @period %>ly</small>
              </div>
              <div class="py-4">
                <div>
                  <%= LogflareWeb.BillingHelpers.sub_button(@plan, @socket, @plans, @period, "Metered") %>
                </div>
                <div>
                  <small class="text-muted">14-day trial</small>
                </div>
              </div>
            </div>
          </div>
          <div class="card card-active-hover text-center py-3 mx-2">
            <div class="card-header-min-height p-4 border-0">
              <h3 class="text-white">Metered BYOB</h3>
            </div>
            <div class="card-body p-0">
              <ul class="list-unstyled">
                <li class="p-2">Unlimited</li>
                <li class="p-2">Unlimited</li>
                <hr />
                <li class="p-2">1 minute</li>
                <li class="p-2">Google Data Studio</li>
                <hr />
                <li class="p-2">BYOB BigQuery<sup>1</sup></li>
                <li class="p-2">Up to 500</li>
                <hr />
                <li class="p-2">Unlimited</li>
                <li class="p-2">Unlimited</li>
                <hr />
                <li class="p-2">Unlimited</li>
                <li class="p-2">Unlimited</li>
              </ul>
              <div class="py-4">
                <h2 class="text-white">
                  <%= Billing.find_plan(@plans, @period, "Metered BYOB").price |> Money.new(:USD) |> Money.to_string(fractional_unit: false) %>
                </h2>
                <small class="text-muted">starts at</small>
                <br />
                <small class="text-muted">per million log events</small>
                <br />
                <small class="text-muted">after one million</small>
                <br />
                <small class="text-muted">paid <%= @period %>ly</small>
              </div>
              <div class="py-4">
                <div>
                  <%= LogflareWeb.BillingHelpers.sub_button(@plan, @socket, @plans, @period, "Metered BYOB") %>
                </div>
                <div>
                  <small class="text-muted">14-day trial</small>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
    <div class="">
      <p class="nam-consectetur-an"><sup>1</sup> Bring Your Own Backend™ to use with Logflare. Give our service account
        access to your Google Cloud Platform account and all reads and writes from Logflare will be performed directly on
        your BigQuery tables. Never archive to object storage again.</p>
    </div>
    """
  end

  def mount(_params, session, socket) do
    socket =
      socket
      |> assign(plan: nil, user: nil, period: "month")
      |> assign(:plans, Billing.list_plans())
      |> assign(:user_id, Map.get(session, "user_id"))
      |> maybe_load_user_and_plan()

    {:ok, socket}
  end

  defp maybe_load_user_and_plan(%{assigns: %{user_id: nil}} = socket), do: socket

  defp maybe_load_user_and_plan(%{assigns: %{user_id: user_id}} = socket) do
    user =
      Users.get(user_id)
      |> Users.preload_sources()
      |> Users.preload_billing_account()

    plan = Billing.get_plan_by_user(user)
    assign(socket, plan: plan, user: user)
  end
end
