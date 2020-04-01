defmodule LogflareWeb.BillingController do
  use LogflareWeb, :controller

  alias Logflare.Billing

  plug LogflareWeb.Plugs.AuthMustBeOwner

  @stripe_publishable_key Application.get_env(:stripity_stripe, :publishable_key)

  def edit(%{assigns: %{user: user}} = conn, _params) do
    billing_account = Billing.get_billing_account_by(user_id: user.id)

    {:ok, %Stripe.Session{} = session} =
      case billing_account do
        nil ->
          Billing.Stripe.create_new_customer_session(user.email)

        _session ->
          Billing.Stripe.create_customer_session(billing_account.stripe_customer)
      end

    conn
    |> put_session(:stripe_session, session)
    |> render("edit.html", stripe_key: @stripe_publishable_key, stripe_session: session)
  end

  def success(%{assigns: %{user: user}} = conn, _params) do
    stripe_session = get_session(conn, :stripe_session)

    case Billing.Stripe.find_completed_session(stripe_session.id) do
      nil ->
        conn
        |> put_flash(
          :error,
          "Something went wrong. Try that again! If this continues please contact support."
        )
        |> redirect(to: Routes.billing_path(conn, :edit))

      stripe_session ->
        IO.inspect(stripe_session, label: "success.stripe_session")

        {:ok, message, _billing_account} =
          Billing.create_or_update_billing_account(user.id, %{
            stripe_customer: stripe_session.customer,
            latest_successful_stripe_session:
              Map.from_struct(stripe_session) |> Map.drop([:display_items])
          })

        conn
        |> put_flash(:info, "Subscription #{message}!")
        |> redirect(to: Routes.billing_path(conn, :edit))
    end
  end

  def abandoned(%{assigns: %{user: _user}} = conn, _params) do
    conn
    |> put_flash(:error, "Abandoned!")
    |> redirect(to: Routes.billing_path(conn, :edit))
  end
end
