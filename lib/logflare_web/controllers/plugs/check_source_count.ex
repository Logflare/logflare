defmodule LogflareWeb.Plugs.CheckSourceCount do
  @moduledoc false
  import Plug.Conn
  import Phoenix.Controller
  alias Logflare.Plans
  alias LogflareWeb.Router.Helpers, as: Routes

  def init(_params) do
  end

  def call(conn, _params) do
    plan = Plans.get_plan_by(stripe_id: get_stripe_plan(conn.assigns.user.billing_account))
    source_count = length(conn.assigns.user.sources)

    if conn.assigns.user.billing_enabled? && source_count >= plan.limit_sources do
      conn
      |> put_flash(
        :error,
        "You have #{source_count} sources. Your limit is #{plan.limit_sources}. Delete one or upgrade first!"
      )
      |> redirect(to: Routes.source_path(conn, :dashboard))
      |> halt()
    else
      conn
    end
  end

  defp get_stripe_plan(billing_account) do
    case billing_account.stripe_subscriptions["data"] do
      nil ->
        free_plan = Plans.get_plan_by(name: "Free")
        free_plan.limit_sources

      [] ->
        free_plan = Plans.get_plan_by(name: "Free")
        free_plan.limit_sources

      list ->
        subscription = hd(list)

        subscription["plan"]["id"]
    end
  end
end
