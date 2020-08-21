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
    if user.billing_enabled? do
      case BillingAccounts.Cache.get_billing_account_by(user_id: user.id) do
        nil ->
          plan = Plans.Cache.get_plan_by(name: "Free")

          conn
          |> assign(:plan, plan)

        billing_account ->
          {:ok, stripe_plan} = BillingAccounts.get_billing_account_stripe_plan(billing_account)

          plan =
            if stripe_plan do
              Plans.Cache.get_plan_by(stripe_id: stripe_plan["id"])
            else
              Plans.Cache.get_plan_by(name: "Free")
            end

          conn
          |> assign(:plan, plan)
      end
    else
      plan = Plans.legacy_plan()

      conn
      |> assign(:plan, plan)
    end
  end
end
