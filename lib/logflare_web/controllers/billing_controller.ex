defmodule LogflareWeb.BillingController do
  use LogflareWeb, :controller

  require Logger

  alias Logflare.Billing
  alias Logflare.Sources.Source
  alias Logflare.{User, Users}
  alias Logflare.Billing.Stripe

  @default_error_message "Something went wrong. Try that again! If this continues please contact support."
  defp env_stripe_publishable_key, do: Application.get_env(:stripity_stripe, :publishable_key)

  def create(%{assigns: %{user: %User{} = user}} = conn, _params) do
    with {:ok, customer} <- Stripe.create_customer(user),
         {{:ok, _billing_account}, _customer} <-
           {Billing.create_billing_account(user, %{
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

  def edit(%{assigns: %{user: %User{billing_account: nil}} = _user} = conn, _params) do
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

  def delete(
        %{assigns: %{user: %User{billing_account: billing_account} = user}} = conn,
        _params
      ) do
    with {:ok, _response} <- Billing.delete_billing_account(user),
         {:ok, _response} <- Stripe.delete_customer(billing_account.stripe_customer) do
      conn
      |> put_flash(:info, "Billing account deleted!")
      |> redirect(to: ~p"/dashboard")
    else
      err ->
        Logger.error("Billing error: #{inspect(err)}", %{billing: %{error_string: inspect(err)}})

        conn
        |> put_flash(:error, @default_error_message)
        |> redirect(to: ~p"/dashboard")
    end
  end

  def confirm_subscription(
        %{assigns: %{user: %User{billing_account: nil} = user}} = conn,
        %{"stripe_id" => _stripe_id} = params
      ) do
    with {:ok, customer} <- Stripe.create_customer(user),
         {:ok, _billing_account} <-
           Billing.create_billing_account(user, %{stripe_customer: customer.id}) do
      user = Users.get(user.id) |> Users.preload_billing_account() |> Users.preload_sources()

      conn
      |> assign(:user, user)
      |> confirm_subscription(params)
    else
      {err, _customer} ->
        Logger.error("Billing error: #{inspect(err)}", %{billing: %{error_string: inspect(err)}})

        conn
        |> put_flash(:error, @default_error_message)
        |> redirect(to: Routes.user_path(conn, :edit))
    end
  end

  def confirm_subscription(
        %{assigns: %{user: %User{billing_account: billing_account} = user}} = conn,
        %{"stripe_id" => stripe_id, "mode" => "payment"}
      ) do
    plan = Billing.get_plan_by(stripe_id: stripe_id)

    with false <- billing_accoount_has_subscription?(billing_account),
         {:ok, session} <- Stripe.create_payment_session(user, plan) do
      conn
      |> put_session(:stripe_session, session)
      |> render("confirm.html", stripe_key: env_stripe_publishable_key(), stripe_session: session)
    else
      true ->
        error_and_redirect(conn, "Please delete your current subscription first!")

      err ->
        Logger.error("Billing error: #{inspect(err)}", %{billing: %{error_string: inspect(err)}})

        error_and_redirect(conn, @default_error_message)
    end
  end

  def confirm_subscription(
        %{assigns: %{user: %User{billing_account: billing_account} = user}} = conn,
        %{"stripe_id" => stripe_id, "type" => "metered"}
      ) do
    with plan <- Billing.get_plan_by(stripe_id: stripe_id),
         false <- billing_accoount_has_subscription?(billing_account),
         false <- billing_account.lifetime_plan,
         {:ok, session} <- Stripe.create_metered_customer_session(user, plan) do
      conn
      |> put_session(:stripe_session, session)
      |> render("confirm.html", stripe_key: env_stripe_publishable_key(), stripe_session: session)
    else
      true ->
        error_and_redirect(conn, "Please delete your current subscription first!")

      err ->
        Logger.error("Billing error: #{inspect(err)}", %{billing: %{error_string: inspect(err)}})

        error_and_redirect(conn, @default_error_message)
    end
  end

  def confirm_subscription(
        %{assigns: %{user: %User{billing_account: billing_account} = user}} = conn,
        %{"stripe_id" => stripe_id}
      ) do
    with plan <- Billing.get_plan_by(stripe_id: stripe_id),
         false <- billing_accoount_has_subscription?(billing_account),
         {:ok, session} <- Stripe.create_customer_session(user, plan) do
      conn
      |> put_session(:stripe_session, session)
      |> render("confirm.html", stripe_key: env_stripe_publishable_key(), stripe_session: session)
    else
      true ->
        error_and_redirect(conn, "Please delete your current subscription first!")

      err ->
        Logger.error("Billing error: #{inspect(err)}", %{billing: %{error_string: inspect(err)}})

        error_and_redirect(conn, @default_error_message)
    end
  end

  def change_subscription(
        %{
          assigns: %{
            user: %User{billing_account: billing_account, sources: sources} = _user,
            plan: %Billing.Plan{type: "standard"}
          }
        } = conn,
        %{"plan" => plan_id, "type" => "metered"}
      ) do
    with plan <- Billing.get_plan!(plan_id),
         true <- billing_accoount_has_subscription?(billing_account),
         {:ok, _response} <- Stripe.change_to_metered_subscription(billing_account, sources, plan) do
      success_and_redirect(conn, "Plan successfully changed!")
    else
      false ->
        error_and_redirect(conn, "You need a subscription to change first!")

      err ->
        Logger.error("Billing error: #{inspect(err)}", %{billing: %{error_string: inspect(err)}})

        error_and_redirect(conn, @default_error_message)
    end
  end

  def change_subscription(
        %{
          assigns: %{
            user: %User{billing_account: billing_account, sources: sources} = _user,
            plan: %Billing.Plan{type: "metered"}
          }
        } = conn,
        %{"plan" => plan_id, "type" => "metered"}
      ) do
    with plan <- Billing.get_plan!(plan_id),
         true <- billing_accoount_has_subscription?(billing_account),
         {:ok, _response} <-
           Stripe.change_from_metered_subscription(billing_account, sources, plan) do
      success_and_redirect(conn, "Plan successfully changed!")
    else
      false ->
        error_and_redirect(conn, "You need a subscription to change first!")

      err ->
        Logger.error("Billing error: #{inspect(err)}", %{billing: %{error_string: inspect(err)}})

        error_and_redirect(conn, @default_error_message)
    end
  end

  def change_subscription(
        %{
          assigns: %{
            user: %User{billing_account: billing_account, sources: sources} = _user,
            plan: %Billing.Plan{type: "standard"}
          }
        } = conn,
        %{"plan" => plan_id, "type" => "standard"}
      ) do
    with plan <- Billing.get_plan!(plan_id),
         true <- billing_accoount_has_subscription?(billing_account),
         {:ok, _response} <- Stripe.change_subscription(billing_account, sources, plan) do
      success_and_redirect(conn, "Plan successfully changed!")
    else
      false ->
        error_and_redirect(conn, "You need a subscription to change first!")

      err ->
        Logger.error("Billing error: #{inspect(err)}", %{billing: %{error_string: inspect(err)}})

        error_and_redirect(conn, @default_error_message)
    end
  end

  def change_subscription(
        %{
          assigns: %{
            user: %User{billing_account: billing_account, sources: sources} = _user,
            plan: %Billing.Plan{type: "metered"}
          }
        } = conn,
        %{"plan" => plan_id, "type" => "standard"}
      ) do
    with plan <- Billing.get_plan!(plan_id),
         true <- billing_accoount_has_subscription?(billing_account),
         {:ok, _response} <-
           Stripe.change_from_metered_subscription(billing_account, sources, plan) do
      success_and_redirect(conn, "Plan successfully changed!")
    else
      false ->
        error_and_redirect(conn, "You need a subscription to change first!")

      err ->
        Logger.error("Billing error: #{inspect(err)}", %{billing: %{error_string: inspect(err)}})

        error_and_redirect(conn, @default_error_message)
    end
  end

  def portal(
        %{assigns: %{user: %User{billing_account: billing_account}} = _user} = conn,
        _params
      ) do
    case Stripe.create_billing_portal_session(billing_account) do
      {:ok, %{url: portal_url}} ->
        conn
        |> redirect(external: portal_url)

      err ->
        Logger.error("Billing error: #{inspect(err)}", %{billing: %{error_string: inspect(err)}})

        error_and_redirect(conn, @default_error_message)
    end
  end

  def update_payment_details(
        %{assigns: %{user: %User{billing_account: billing_account}} = _user} = conn,
        _params
      ) do
    with true <- billing_accoount_has_subscription?(billing_account),
         {:ok, session} <- Stripe.create_add_credit_card_session(billing_account) do
      conn
      |> put_session(:stripe_session, session)
      |> render("confirm.html", stripe_key: env_stripe_publishable_key(), stripe_session: session)
    else
      false ->
        error_and_redirect(conn, "Please subscribe first!")

      err ->
        Logger.error("Billing error: #{inspect(err)}", %{billing: %{error_string: inspect(err)}})

        error_and_redirect(conn, @default_error_message)
    end
  end

  def unsubscribe(
        %{assigns: %{user: %User{billing_account: billing_account} = user}} = conn,
        %{"id" => stripe_subscription_id}
      ) do
    with {:ok, subscription} <-
           get_billing_account_subscription(billing_account, stripe_subscription_id),
         {:ok, _response} <- Stripe.delete_subscription(subscription["id"]),
         {:ok, _billing_account} <- Billing.sync_subscriptions(billing_account) do
      Source.Supervisor.reset_all_user_sources(user)

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
        %{assigns: %{user: %User{billing_account: billing_account}} = _user} = conn,
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
           Billing.update_billing_account(billing_account, billing_params) do
      success_and_redirect(conn, "Payment method updated!")
    else
      err ->
        Logger.error("Billing error: #{inspect(err)}", %{billing: %{error_string: inspect(err)}})

        error_and_redirect(conn)
    end
  end

  def success(
        %{assigns: %{user: %User{billing_account: billing_account} = user}} = conn,
        _params
      ) do
    stripe_session = get_session(conn, :stripe_session)

    with {:ok, session} <-
           Stripe.find_completed_session(stripe_session.id),
         {:ok, _billing_account} <-
           Billing.update_billing_account(billing_account, %{
             latest_successful_stripe_session: session
           }) do
      Source.Supervisor.reset_all_user_sources(user)

      success_and_redirect(conn, "Subscription created!")
    else
      err ->
        Logger.error("Billing error: #{inspect(err)}", %{billing: %{error_string: inspect(err)}})

        error_and_redirect(conn)
    end
  end

  def sync(%{assigns: %{user: %User{billing_account: billing_account}} = _user} = conn, _params) do
    case Billing.sync_billing_account(billing_account) do
      {:ok, _billing_account} ->
        success_and_redirect(conn, "Billing account synced!")

      err ->
        Logger.error("Billing error: #{inspect(err)}", %{billing: %{error_string: inspect(err)}})

        error_and_redirect(conn)
    end
  end

  def abandoned(%{assigns: %{user: _user}} = conn, _params) do
    conn
    |> put_flash(:error, "Abandoned!")
    |> redirect(to: ~p"/billing/edit")
  end

  defp success_and_redirect(conn, message) do
    conn
    |> put_flash(:info, "Success! #{message}")
    |> redirect(to: ~p"/billing/edit")
  end

  defp error_and_redirect(conn, message \\ @default_error_message) do
    conn
    |> put_flash(:error, message)
    |> redirect(to: ~p"/billing/edit")
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
