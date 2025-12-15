defmodule LogflareWeb.BillingHelpers do
  @moduledoc false
  use PhoenixHTMLHelpers

  alias Logflare.Billing
  alias LogflareWeb.Router.Helpers, as: Routes

  def sub_button(plan, socket, plans, period, plan_name) do
    new_plan = Billing.find_plan(plans, period, plan_name)

    cond do
      is_nil(plan) ->
        link("Start trial",
          to:
            Routes.billing_path(socket, :confirm_subscription, %{
              "stripe_id" => new_plan.stripe_id,
              "type" => new_plan.type
            }),
          class: "btn btn-dark text-white form-button btn w-75 mr-0 mb-1"
        )

      plan.name == "Lifetime" ->
        link("Contact us",
          to: Routes.marketing_path(socket, :contact),
          class: "btn btn-dark text-white form-button w-75 mr-0 mb-1"
        )

      plan.name == "Free" || plan.name == "Legacy" ->
        link("Subscribe",
          to:
            Routes.billing_path(socket, :confirm_subscription, %{
              "stripe_id" => new_plan.stripe_id,
              "type" => new_plan.type
            }),
          class: "btn btn-dark text-white form-button w-75 mr-0 mb-1"
        )

      plan.id == new_plan.id ->
        link("Subscribed",
          to:
            Routes.billing_path(socket, :confirm_subscription, %{
              "stripe_id" => new_plan.stripe_id,
              "type" => new_plan.type
            }),
          class: "btn btn-dark text-white form-button disabled w-75 mr-0 mb-1"
        )

      plan.name == new_plan.name ->
        link("Switch to #{period}ly",
          to:
            Routes.billing_path(socket, :change_subscription, %{
              "plan" => new_plan.id,
              "type" => new_plan.type
            }),
          class: "btn btn-dark text-white form-button w-75 mr-0 mb-1"
        )

      plan.id > new_plan.id ->
        link("Downgrade",
          to:
            Routes.billing_path(socket, :change_subscription, %{
              "plan" => new_plan.id,
              "type" => new_plan.type
            }),
          class: "btn btn-dark text-white form-button w-75 mr-0 mb-1"
        )

      plan.id < new_plan.id ->
        link("Upgrade",
          to:
            Routes.billing_path(socket, :change_subscription, %{
              "plan" => new_plan.id,
              "type" => new_plan.type
            }),
          class: "btn btn-dark text-white form-button w-75 mr-0 mb-1"
        )

      true ->
        link("Contact Us",
          to: Routes.marketing_path(socket, :contact),
          class: "btn btn-dark text-white form-button w-75 mr-0 mb-1"
        )
    end
  end
end
