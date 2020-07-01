defmodule LogflareWeb.BillingController do
  use LogflareWeb, :controller

  require Logger

  alias Logflare.BillingAccounts
  alias Logflare.Plans
  alias Logflare.BillingAccounts.Stripe

  plug LogflareWeb.Plugs.AuthMustBeOwner

  @stripe_publishable_key Application.get_env(:stripity_stripe, :publishable_key)
  @default_error_message "Something went wrong. Try that again! If this continues please contact support."

  def create(%{assigns: %{user: user}} = conn, _params) do
    with {:ok, customer} <- Stripe.create_customer(user),
         {{:ok, _billing_account}, _customer} <-
           {BillingAccounts.create_billing_account(user, %{
              stripe_customer: customer.id
            }), customer} do
      success_and_redirect(conn, "Billing account created!")
    else
      {err, customer} ->
        Logger.error("Billing error: #{inspect(err)}", %{billing: %{error_string: inspect(err)}})

        {:ok, _response} = Stripe.delete_customer(customer["id"])

        conn
        |> put_flash(:error, @default_error_message)
        |> redirect(to: Routes.user_path(conn, :edit))
    end
  end

  def edit(%{assigns: %{user: %{billing_account: nil}} = _user} = conn, _params) do
    conn
    |> put_flash(
      :error,
      [
        "Please ",
        Phoenix.HTML.Link.link("create a billing account",
          to: Routes.billing_path(conn, :create),
          method: :post
        ),
        " first!"
      ]
    )
    |> redirect(to: Routes.user_path(conn, :edit) <> "#billing-account")
  end

  def edit(%{assigns: %{user: _user}} = conn, _params) do
    conn
    |> render("edit.html")
  end

  def delete(%{assigns: %{user: %{billing_account: billing_account}} = _user} = conn, _params) do
    with {:ok, _response} <- Stripe.delete_customer(billing_account.stripe_customer),
         {:ok, _response} <- BillingAccounts.delete_billing_account(billing_account) do
      conn
      |> put_flash(:info, "Billing account deleted!")
      |> redirect(to: Routes.user_path(conn, :edit))
    else
      err ->
        Logger.error("Billing error: #{inspect(err)}", %{billing: %{error_string: inspect(err)}})

        conn
        |> put_flash(:error, @default_error_message)
        |> redirect(to: Routes.user_path(conn, :edit))
    end
  end

  def confirm_subscription(
        %{assigns: %{user: %{billing_account: billing_account} = user}} = conn,
        %{"stripe_id" => stripe_id}
      ) do
    with plan <- Plans.get_plan_by(stripe_id: stripe_id),
         false <- billing_accoount_has_subscription?(billing_account),
         {:ok, session} <- Stripe.create_customer_session(user, plan) do
      conn
      |> put_session(:stripe_session, session)
      |> render("confirm.html", stripe_key: @stripe_publishable_key, stripe_session: session)
    else
      true ->
        error_and_redirect(conn, "Please delete your current subscription first!")

      err ->
        Logger.error("Billing error: #{inspect(err)}", %{billing: %{error_string: inspect(err)}})

        error_and_redirect(conn, @default_error_message)
    end
  end

  def portal(
        %{assigns: %{user: %{billing_account: billing_account}} = _user} = conn,
        _params
      ) do
    with {:ok, %{url: portal_url}} <- Stripe.create_billing_portal_session(billing_account) do
      conn
      |> redirect(external: portal_url)
    else
      err ->
        Logger.error("Billing error: #{inspect(err)}", %{billing: %{error_string: inspect(err)}})

        error_and_redirect(conn, @default_error_message)
    end
  end

  def change_subscription(
        %{assigns: %{user: %{billing_account: billing_account}} = _user} = conn,
        _params
      ) do
    with true <- billing_accoount_has_subscription?(billing_account),
         {:ok, session} <- Stripe.create_add_credit_card_session(billing_account) do
      conn
      |> put_session(:stripe_session, session)
      |> render("confirm.html", stripe_key: @stripe_publishable_key, stripe_session: session)
    else
      false ->
        error_and_redirect(conn, "Please subscribe first!")

      err ->
        Logger.error("Billing error: #{inspect(err)}", %{billing: %{error_string: inspect(err)}})

        error_and_redirect(conn, @default_error_message)
    end
  end

  def unsubscribe(
        %{assigns: %{user: %{billing_account: billing_account}} = _user} = conn,
        %{"id" => stripe_subscription_id}
      ) do
    with {:ok, subscription} <-
           get_billing_account_subscription(billing_account, stripe_subscription_id),
         {:ok, _response} <- Stripe.delete_subscription(subscription["id"]),
         {:ok, _billing_account} <- BillingAccounts.sync_subscriptions(billing_account) do
      success_and_redirect(conn, "Subscription deleted!")
    else
      {:error, :subscription_not_found} ->
        error_and_redirect(conn, "Subscription not found.")

      err ->
        Logger.error("Billing error: #{inspect(err)}", %{billing: %{error_string: inspect(err)}})

        error_and_redirect(conn)
    end
  end

  def update_credit_card_success(
        %{assigns: %{user: %{billing_account: billing_account}} = _user} = conn,
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
         {:ok, _billing_account} <-
           BillingAccounts.update_billing_account(billing_account, billing_params) do
      success_and_redirect(conn, "Payment method updated!")
    else
      err ->
        Logger.error("Billing error: #{inspect(err)}", %{billing: %{error_string: inspect(err)}})

        error_and_redirect(conn)
    end
  end

  def success(%{assigns: %{user: %{billing_account: billing_account}} = _user} = conn, _params) do
    stripe_session = get_session(conn, :stripe_session)
    billing_params = %{latest_successful_stripe_session: stripe_session}

    with {:ok, _stripe_session} <-
           Stripe.find_completed_session(stripe_session.id),
         {:ok, _billing_account} <-
           BillingAccounts.update_billing_account(billing_account, billing_params) do
      success_and_redirect(conn, "Subscription created!")
    else
      err ->
        Logger.error("Billing error: #{inspect(err)}", %{billing: %{error_string: inspect(err)}})

        error_and_redirect(conn)
    end
  end

  def sync(%{assigns: %{user: %{billing_account: billing_account}} = _user} = conn, _params) do
    with {:ok, _billing_account} <- BillingAccounts.sync_billing_account(billing_account) do
      success_and_redirect(conn, "Billing account synced!")
    else
      err ->
        Logger.error("Billing error: #{inspect(err)}", %{billing: %{error_string: inspect(err)}})

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

  defp get_billing_account_subscription(billing_account, stripe_subscription_id) do
    subscription =
      billing_account.stripe_subscriptions["data"]
      |> Enum.find(fn sub -> sub["id"] == stripe_subscription_id end)

    case subscription do
      nil ->
        {:error, :subscription_not_found}

      _else ->
        {:ok, subscription}
    end
  end

  defp billing_accoount_has_subscription?(billing_account) do
    if subcriptions = billing_account.stripe_subscriptions["data"] do
      Enum.count(subcriptions) > 0
    else
      false
    end
  end
end
