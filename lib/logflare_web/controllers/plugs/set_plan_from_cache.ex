defmodule LogflareWeb.Plugs.SetPlanFromCache do
  @moduledoc """
  Assigns team user if browser session is present in conn
  """
  import Plug.Conn

  alias Logflare.BillingAccounts
  alias Logflare.User
  alias Logflare.Plans

  def init(_), do: nil

  def call(%{assigns: %{user: %User{}}} = conn, opts), do: set_plan(conn, opts)

  def call(conn, _opts), do: conn

  defp set_plan(%{assigns: %{user: %User{} = user}} = conn, _opts) do
    plan = get_plan(user)

    conn
    |> assign(:plan, plan)
  end

  defp get_plan(user) do
    if user.billing_enabled do
      ba = BillingAccounts.get_billing_account_by(user_id: user.id)

      case ba do
        nil ->
          Plans.get_plan_by(name: "Free")

        %BillingAccounts.BillingAccount{lifetime_plan: true} ->
          Plans.get_plan_by(name: "Lifetime")

        %BillingAccounts.BillingAccount{stripe_subscriptions: nil} ->
          Plans.get_plan_by(name: "Free")

        billing_account ->
          case BillingAccounts.get_billing_account_stripe_plan(billing_account) do
            {:ok, nil} ->
              Plans.get_plan_by(name: "Free")

            {:ok, stripe_plan} ->
              Plans.get_plan_by(stripe_id: stripe_plan["id"])
          end
      end
    else
      Plans.legacy_plan()
    end
  end
end
