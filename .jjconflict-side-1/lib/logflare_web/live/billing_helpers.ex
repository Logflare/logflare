defmodule LogflareWeb.BillingHelpers do
  @moduledoc false

  use LogflareWeb, :html
  use LogflareWeb, :routes

  alias Logflare.Billing

  def sub_button(plan, _socket, plans, period, plan_name) do
    new_plan = Billing.find_plan(plans, period, plan_name)

    cond do
      is_nil(plan) ->
        link("Start trial",
          to:
            ~p"/billing/subscription/confirm?#{%{"stripe_id" => new_plan.stripe_id, "type" => new_plan.type}}",
          class: "btn btn-dark text-white form-button btn w-75 mr-0 mb-1"
        )

      plan.name == "Lifetime" ->
        link("Contact us",
          to: ~p"/contact",
          class: "btn btn-dark text-white form-button w-75 mr-0 mb-1"
        )

      plan.name == "Free" || plan.name == "Legacy" ->
        link("Subscribe",
          to:
            ~p"/billing/subscription/confirm?#{%{"stripe_id" => new_plan.stripe_id, "type" => new_plan.type}}",
          class: "btn btn-dark text-white form-button w-75 mr-0 mb-1"
        )

      plan.id == new_plan.id ->
        link("Subscribed",
          to:
            ~p"/billing/subscription/confirm?#{%{"stripe_id" => new_plan.stripe_id, "type" => new_plan.type}}",
          class: "btn btn-dark text-white form-button disabled w-75 mr-0 mb-1"
        )

      plan.name == new_plan.name ->
        link("Switch to #{period}ly",
          to:
            ~p"/billing/subscription/change?#{%{"plan" => new_plan.id, "type" => new_plan.type}}",
          class: "btn btn-dark text-white form-button w-75 mr-0 mb-1"
        )

      plan.id > new_plan.id ->
        link("Downgrade",
          to:
            ~p"/billing/subscription/change?#{%{"plan" => new_plan.id, "type" => new_plan.type}}",
          class: "btn btn-dark text-white form-button w-75 mr-0 mb-1"
        )

      plan.id < new_plan.id ->
        link("Upgrade",
          to:
            ~p"/billing/subscription/change?#{%{"plan" => new_plan.id, "type" => new_plan.type}}",
          class: "btn btn-dark text-white form-button w-75 mr-0 mb-1"
        )

      true ->
        link("Contact Us",
          to: ~p"/contact",
          class: "btn btn-dark text-white form-button w-75 mr-0 mb-1"
        )
    end
  end
end
