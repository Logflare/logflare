defmodule LogflareWeb.BillingController do
  use LogflareWeb, :controller

  alias Logflare.Billing

  plug LogflareWeb.Plugs.AuthMustBeOwner

  @stripe_publishable_key Application.get_env(:stripity_stripe, :publishable_key)

  def start(%{assigns: %{user: user}} = conn, _params) do
    conn
    |> render("start.html")
  end

  def edit(%{assigns: %{user: user}} = conn, _params) do
    conn
    |> render("edit.html")
  end

  def confirm(%{assigns: %{user: user}} = conn, %{"plan_id" => plan_id}) do
    billing_account = Billing.get_billing_account_by(user_id: user.id)

    {:ok, %Stripe.Session{} = session} =
      case billing_account do
        nil ->
          Billing.Stripe.create_new_customer_session(user.email, plan_id)

        _session ->
          Billing.Stripe.create_customer_session(billing_account)
      end

    conn
    |> put_session(:stripe_session, session)
    |> put_session(:plan_id, plan_id)
    |> render("confirm.html", stripe_key: @stripe_publishable_key, stripe_session: session)
  end

  def success(%{assigns: %{user: user}} = conn, _params) do
    stripe_session = get_session(conn, :stripe_session)
    plan_id = get_session(conn, :plan_id) |> IO.inspect()

    case Billing.Stripe.find_completed_session(stripe_session.id) do
      nil ->
        conn
        |> put_flash(
          :error,
          "Something went wrong. Try that again! If this continues please contact support."
        )
        |> redirect(to: Routes.billing_path(conn, :edit))

      %Stripe.Event{data: %{object: %Stripe.Session{} = stripe_session}} ->
        {:ok, message, _billing_account} =
          Billing.create_or_update_billing_account(user, %{
            plan_id: plan_id,
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
