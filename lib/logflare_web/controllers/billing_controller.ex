defmodule LogflareWeb.BillingController do
  use LogflareWeb, :controller

  alias Logflare.Billing
  alias Logflare.Plans
  alias Logflare.Billing.Stripe

  plug LogflareWeb.Plugs.AuthMustBeOwner

  @stripe_publishable_key Application.get_env(:stripity_stripe, :publishable_key)
  @default_error_message "Something went wrong. Try that again! If this continues please contact support."

  def create(%{assigns: %{user: user}} = conn, _params) do
    with {:ok, customer} <- Stripe.create_customer(user),
         {{:ok, _billing_account}, customer} <-
           {Billing.create_billing_account(user, %{
              stripe_customer: customer.id
            }), customer} do
      success_and_redirect(conn, "Billing account created!")
    else
      {_err, customer} ->
        {:ok, _response} = Stripe.delete_customer(customer.id)

        conn
        |> put_flash(:error, @default_error_message)
        |> redirect(to: Routes.user_path(conn, :edit))
    end
  end

  def edit(%{assigns: %{user: %{billing_account: nil}} = user} = conn, _params) do
    conn
    |> put_flash(:error, "Please creaete a billing account first!")
    |> redirect(to: Routes.user_path(conn, :edit) <> "#billing-account")
  end

  def edit(%{assigns: %{user: user}} = conn, _params) do
    conn
    |> render("edit.html")
  end

  def delete(%{assigns: %{user: %{billing_account: billing_account}} = user} = conn, _params) do
    with {:ok, _response} <- Stripe.delete_customer(billing_account.stripe_customer),
         {:ok, _response} <- Billing.delete_billing_account(billing_account) do
      conn
      |> put_flash(:info, "Billing account deleted!")
      |> redirect(to: Routes.user_path(conn, :edit))
    else
      _err ->
        conn
        |> put_flash(:error, @default_error_message)
        |> redirect(to: Routes.user_path(conn, :edit))
    end
  end

  def subscribe(%{assigns: %{user: user}} = conn, _params) do
    conn
    |> render("start.html")
  end

  def preview_subscription(
        %{assigns: %{user: %{billing_account: billing_account}} = user} = conn,
        %{
          "plan_id" => plan_id
        }
      ) do
    with plan <- Plans.get_plan!(plan_id),
         {:error, :not_found} <- get_billing_account_subscription(billing_account),
         {:ok, session} <- Stripe.create_customer_session(billing_account, plan) do
      conn
      |> put_session(:stripe_session, session)
      |> render("confirm.html", stripe_key: @stripe_publishable_key, stripe_session: session)
    else
      {:ok, _subscription} ->
        error_and_redirect(conn, "Please delete your current subscription first!")

      _err ->
        error_and_redirect(conn, @default_error_message)
    end
  end

  def change_subscription(
        %{assigns: %{user: %{billing_account: billing_account}} = user} = conn,
        _params
      ) do
    with {:ok, subscription} <- get_billing_account_subscription(billing_account),
         {:ok, session} <- Stripe.create_add_credit_card_session(billing_account) do
      conn
      |> put_session(:stripe_session, session)
      |> render("confirm.html", stripe_key: @stripe_publishable_key, stripe_session: session)
    else
      {:error, :not_found} ->
        error_and_redirect(conn, "Please subscribe first!")

      _err ->
        error_and_redirect(conn, @default_error_message)
    end
  end

  def unsubscribe(%{assigns: %{user: %{billing_account: billing_account}} = user} = conn, _params) do
    with {:ok, subscription} <- get_billing_account_subscription(billing_account),
         {:ok, _response} <- Stripe.delete_subscription(subscription["id"]),
         {:ok, _billing_account} <-
           Billing.update_billing_account(billing_account, %{stripe_subscriptions: nil}) do
      success_and_redirect(conn, "Subscription deleted!")
    else
      {:error, :not_found} ->
        error_and_redirect(conn, "Subscription not found.")

      _err ->
        error_and_redirect(conn)
    end
  end

  def update_credit_card_success(
        %{assigns: %{user: %{billing_account: billing_account}} = user} = conn,
        _params
      ) do
    stripe_session = get_session(conn, :stripe_session)
    billing_params = %{latest_successful_stripe_session: stripe_session}

    with {:ok, stripe_session} <-
           Stripe.find_completed_session(stripe_session.id),
         {:ok, setup_intent} <-
           Stripe.get_setup_intent(stripe_session.setup_intent),
         {:ok, _response} <-
           Stripe.update_customer(billing_account.stripe_customer, %{
             invoice_settings: %{default_payment_method: setup_intent.payment_method}
           }),
         {:ok, subscription} <-
           get_billing_account_subscription(billing_account),
         {:ok, _response} <-
           Stripe.update_subscription(subscription["id"], %{
             default_payment_method: setup_intent.payment_method
           }),
         {:ok, _billing_account} <-
           Billing.sync_billing_account(billing_account, billing_params) do
      success_and_redirect(conn, "Subscription created!")
    else
      _err ->
        error_and_redirect(conn)
    end
  end

  def success(%{assigns: %{user: %{billing_account: billing_account}} = user} = conn, _params) do
    stripe_session = get_session(conn, :stripe_session)
    billing_params = %{latest_successful_stripe_session: stripe_session}

    with {:ok, stripe_session} <-
           Stripe.find_completed_session(stripe_session.id),
         {:ok, _billing_account} <-
           Billing.sync_billing_account(billing_account, billing_params) do
      success_and_redirect(conn, "Subscription created!")
    else
      _err ->
        error_and_redirect(conn)
    end
  end

  def abandoned(%{assigns: %{user: _user}} = conn, _params) do
    conn
    |> put_flash(:error, "Abandoned!")
    |> redirect(to: Routes.billing_path(conn, :edit))
  end

  defp success_and_redirect(conn, message) do
    conn
    |> put_flash(:info, "Success! #{message}")
    |> redirect(to: Routes.billing_path(conn, :edit))
  end

  defp error_and_redirect(conn, message \\ @default_error_message) do
    conn
    |> put_flash(:error, message)
    |> redirect(to: Routes.billing_path(conn, :edit))
  end

  defp get_billing_account_subscription(billing_account) do
    # we only support one subscription currently
    case billing_account.stripe_subscriptions["data"] do
      nil ->
        {:error, :not_found}

      [subscription] ->
        {:ok, subscription}

      _else ->
        {:error, :uknown_error}
    end
  end
end
