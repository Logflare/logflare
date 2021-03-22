defmodule LogflareWeb.Plugs.CheckSourceCount do
  @moduledoc false
  import Plug.Conn
  import Phoenix.Controller
  alias LogflareWeb.Router.Helpers, as: Routes
  alias Logflare.BillingAccounts

  def init(_params) do
  end

  def call(%{assigns: %{user: user, plan: _plan}, method: "DELETE"} = conn, _params),
    do: update_stripe_count_and_return(conn, length(user.sources))

  def call(%{assigns: %{user: user, plan: plan}} = conn, _params) do
    if user.billing_enabled do
      source_count = length(user.sources)

      if source_count >= plan.limit_sources do
        conn
        |> put_flash(
          :error,
          "You have #{source_count} sources. Your limit is #{plan.limit_sources}. Delete one or upgrade first!"
        )
        |> redirect(to: Routes.source_path(conn, :dashboard))
        |> halt()
      else
        update_stripe_count_and_return(conn, source_count)
      end
    else
      conn
    end
  end

  def call(conn, _params), do: conn

  defp update_stripe_count_and_return(
         %{assigns: %{user: user}, method: "POST"} = conn,
         source_count
       ) do
    {:ok, item} =
      BillingAccounts.get_billing_account_by(user_id: user.id)
      |> BillingAccounts.get_billing_account_stripe_subscription_item()

    if item do
      BillingAccounts.Stripe.update_subscription_item(item["id"], %{quantity: source_count + 1})
    end

    conn
  end

  defp update_stripe_count_and_return(
         %{assigns: %{user: user}, method: "DELETE"} = conn,
         source_count
       ) do
    {:ok, item} =
      BillingAccounts.get_billing_account_by(user_id: user.id)
      |> BillingAccounts.get_billing_account_stripe_subscription_item()

    if item do
      BillingAccounts.Stripe.update_subscription_item(item["id"], %{quantity: source_count - 1})
    end

    conn
  end
end
