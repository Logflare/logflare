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

  defp set_plan(%{assigns: %{user: user}} = conn, _opts) do
    plan = get_plan(user)

    conn
    |> assign(:plan, plan)
  end

  defp get_plan(user) do
    if user.billing_enabled? do
      ba = BillingAccounts.Cache.get_billing_account_by(user_id: user.id)

      case ba do
        nil ->
          Plans.Cache.get_plan_by(name: "Free")

        %BillingAccounts.BillingAccount{stripe_subscriptions: nil} ->
          Plans.Cache.get_plan_by(name: "Free")

        %BillingAccounts.BillingAccount{lifetime_plan?: true} ->
          Plans.Cache.get_plan_by(name: "Lifetime")

        billing_account ->
          case BillingAccounts.Cache.get_billing_account_stripe_plan(billing_account) do
            {:ok, nil} ->
              Plans.Cache.get_plan_by(name: "Free")

            {:ok, stripe_plan} ->
              Plans.Cache.get_plan_by(stripe_id: stripe_plan["id"])
          end
      end
    else
      Plans.legacy_plan()
    end
  end
end
