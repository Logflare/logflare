defmodule LogflareWeb.UserController do
  use LogflareWeb, :controller

  use PhoenixHTMLHelpers

  require Logger

  plug LogflareWeb.Plugs.AuthMustBeOwner

  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Billing.Stripe
  alias Logflare.Sources.Source.Supervisor
  alias Logflare.TeamUsers
  alias Logflare.User
  alias Logflare.Users

  @bq_fields ~w(
    bigquery_project_id
    bigquery_dataset_location
    bigquery_dataset_id
    bigquery_reservation_alerts
    bigquery_reservation_search
    bigquery_processed_bytes_limit
    bigquery_enable_managed_service_accounts
  )a

  def api_show(%{assigns: %{user: user}} = conn, _params) do
    conn
    |> json(user)
  end

  def edit(%{assigns: %{user: user}} = conn, _params) do
    changeset = User.user_allowed_changeset(user, %{})

    render(conn, "edit.html",
      changeset: changeset,
      user: user,
      service_account: env_service_account()
    )
  end

  def update(%{assigns: %{plan: %{name: "Free"}, user: user}} = conn, %{
        "user" => %{"bigquery_project_id" => _id}
      }) do
    Logger.warning("user audit: BigQuery backend update blocked for free plan",
      action: "user.bq_update_blocked",
      user_id: user.id,
      user_email: user.email
    )

    message = [
      "Please ",
      PhoenixHTMLHelpers.Link.link("upgrade to a paid plan",
        to: ~p"/billing/edit"
      ),
      " to setup your own BigQuery backend."
    ]

    conn
    |> put_flash(:error, message)
    |> redirect(to: ~p"/account/edit")
  end

  def update(%{assigns: %{user: user}} = conn, %{"user" => params}) do
    case Users.update_user_allowed(user, params) do
      {:ok, updated_user} ->
        Logger.info("user audit: user account updated",
          action: "user.update",
          user_id: user.id,
          user_email: user.email,
          params: inspect(params)
        )

        bq_changes = bq_field_changes(user, updated_user)

        if bq_changes != %{} do
          Logger.info("user audit: BigQuery backend settings changed",
            action: "user.bq_settings_updated",
            user_id: user.id,
            user_email: user.email,
            changes: inspect(bq_changes)
          )
        end

        if updated_user.bigquery_project_id != user.bigquery_project_id,
          do: Supervisor.reset_all_user_sources(user)

        if updated_user.bigquery_enable_managed_service_accounts do
          # update iam policy
          BigQueryAdaptor.update_iam_policy(updated_user)
        end

        conn
        |> put_flash(:info, "Account updated!")
        |> redirect(to: ~p"/account/edit")

      {:error, changeset} ->
        Logger.warning("user audit: user account update failed",
          action: "user.update_failed",
          user_id: user.id,
          user_email: user.email,
          params: inspect(params),
          errors: inspect(changeset.errors)
        )

        conn
        |> put_flash(:error, "Something went wrong! See below for errors.")
        |> put_status(406)
        |> render("edit.html",
          changeset: changeset,
          user: conn.assigns.user,
          service_account: env_service_account()
        )
    end
  end

  def delete(
        %{assigns: %{user: %User{billing_account: %{stripe_customer: stripe_customer}} = user}} =
          conn,
        _params
      ) do
    Logger.info("user audit: user account deletion requested",
      action: "user.delete",
      user_id: user.id,
      user_email: user.email,
      has_stripe_customer: true
    )

    with {:ok, _user} <- Users.delete_user(user),
         {:ok, _response} <- Stripe.delete_customer(stripe_customer) do
      Logger.info("user audit: user account deleted",
        action: "user.deleted",
        user_id: user.id,
        user_email: user.email
      )

      conn
      |> configure_session(drop: true)
      |> redirect(to: ~p"/auth/login?#{%{user_deleted: true}}")
    else
      _err ->
        Logger.error("user audit: user account deletion failed",
          action: "user.delete_failed",
          user_id: user.id,
          user_email: user.email
        )

        conn
        |> put_flash(
          :error,
          "Something went wrong! Please try again and contact support if this continues."
        )
        |> render("edit.html")
    end
  end

  def delete(%{assigns: %{user: user}} = conn, _params) do
    Logger.info("user audit: user account deletion requested",
      action: "user.delete",
      user_id: user.id,
      user_email: user.email,
      has_stripe_customer: false
    )

    case Users.delete_user(user) do
      {:ok, _user} ->
        Logger.info("user audit: user account deleted",
          action: "user.deleted",
          user_id: user.id,
          user_email: user.email
        )

        conn
        |> configure_session(drop: true)
        |> redirect(to: ~p"/auth/login?#{%{user_deleted: true}}")

      _err ->
        Logger.error("user audit: user account deletion failed",
          action: "user.delete_failed",
          user_id: user.id,
          user_email: user.email
        )

        conn
        |> put_flash(
          :error,
          "Something went wrong! Please try again and contact support if this continues."
        )
        |> render("edit.html")
    end
  end

  def new_api_key(conn, _params) do
    case conn.params["undo"] do
      "true" ->
        %{assigns: %{user: user}} = conn
        new_api_key = user.old_api_key
        old_api_key = user.api_key
        auth_params = %{api_key: new_api_key, old_api_key: old_api_key}

        Users.update_user_all_fields(user, auth_params)

        Logger.info("user audit: API key restored",
          action: "user.api_key_restored",
          user_id: user.id,
          user_email: user.email
        )

        conn
        |> put_flash(:info, "API key restored!")
        |> redirect(to: ~p"/dashboard")

      nil ->
        %{assigns: %{user: user}} = conn
        new_api_key = :crypto.strong_rand_bytes(12) |> Base.url_encode64() |> binary_part(0, 12)
        old_api_key = user.api_key
        auth_params = %{api_key: new_api_key, old_api_key: old_api_key}

        Users.update_user_all_fields(user, auth_params)

        Logger.info("user audit: API key reset",
          action: "user.api_key_reset",
          user_id: user.id,
          user_email: user.email
        )

        conn
        |> put_flash(:info, [
          "API key reset! ",
          link("Undo?", to: Routes.user_path(conn, :new_api_key, undo: true))
        ])
        |> redirect(to: ~p"/dashboard")
    end
  end

  def change_owner(%{assigns: %{user: _user}} = conn, %{"user" => %{"team_user_id" => ""}}) do
    conn
    |> put_flash(:error, "Please select a team member!")
    |> redirect(to: Routes.user_path(conn, :edit) <> "#change-account-owner")
  end

  def change_owner(%{assigns: %{user: user}} = conn, %{"user" => %{"team_user_id" => id}}) do
    team_user = TeamUsers.get_team_user(id)
    user = Users.preload_team(user)

    if is_nil(team_user) or team_user.team_id != user.team.id do
      Logger.warning("user audit: unauthorized ownership transfer attempt",
        action: "user.change_owner_unauthorized",
        user_id: user.id,
        user_email: user.email,
        target_team_user_id: id
      )

      conn
      |> put_flash(:error, "Not authorized to transfer ownership to this team member")
      |> redirect(to: Routes.user_path(conn, :edit) <> "#change-account-owner")
    else
      with {:ok, new_owner} <- Users.change_owner(team_user, user),
           {:ok, _resp} <- TeamUsers.delete_team_user(team_user) do
        if user.billing_account do
          Stripe.update_customer(user.billing_account.stripe_customer, %{email: new_owner.email})
        end

        Logger.info("user audit: account ownership transferred",
          action: "user.change_owner",
          user_id: user.id,
          user_email: user.email,
          new_owner_id: new_owner.id,
          new_owner_email: new_owner.email,
          team_user_id: id
        )

        conn
        |> put_flash(:info, "Owner successfully changed!")
        |> redirect(to: Routes.user_path(conn, :edit) <> "#team-members")
      else
        {:error, changeset} ->
          Logger.error("user audit: ownership transfer failed",
            action: "user.change_owner_failed",
            user_id: user.id,
            user_email: user.email,
            target_team_user_id: id,
            errors: inspect(changeset.errors)
          )

          case List.first(changeset.errors) do
            {:email, {"has already been taken", _data}} ->
              conn
              |> put_flash(
                :error,
                "This email address is associated with a Logflare account already. Login with this user and delete the `Account` then try again."
              )
              |> redirect(to: Routes.user_path(conn, :edit) <> "#change-account-owner")

            _ ->
              conn
              |> put_flash(
                :error,
                "Something went wrong. Please contact support if this continues."
              )
              |> redirect(to: Routes.user_path(conn, :edit) <> "#change-account-owner")
          end

        _err ->
          Logger.error("user audit: ownership transfer failed",
            action: "user.change_owner_failed",
            user_id: user.id,
            user_email: user.email,
            target_team_user_id: id
          )

          conn
          |> put_flash(:error, "Something went wrong. Please contact support if this continues.")
          |> redirect(to: Routes.user_path(conn, :edit) <> "#change-account-owner")
      end
    end
  end

  defp env_service_account,
    do: Application.get_env(:logflare, Logflare.Google)[:service_account] || ""

  defp bq_field_changes(old_user, new_user) do
    Enum.reduce(@bq_fields, %{}, fn field, acc ->
      old_val = Map.get(old_user, field)
      new_val = Map.get(new_user, field)

      if old_val != new_val do
        Map.put(acc, field, %{from: old_val, to: new_val})
      else
        acc
      end
    end)
  end
end
