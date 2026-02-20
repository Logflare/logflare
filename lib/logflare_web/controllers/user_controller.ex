defmodule LogflareWeb.UserController do
  use LogflareWeb, :controller

  use PhoenixHTMLHelpers

  plug LogflareWeb.Plugs.AuthMustBeTeamAdmin

  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Billing.Stripe
  alias Logflare.Sources.Source.Supervisor
  alias Logflare.TeamUsers
  alias Logflare.User
  alias Logflare.Users

  defp env_service_account,
    do: Application.get_env(:logflare, Logflare.Google)[:service_account] || ""

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

  def update(%{assigns: %{plan: %{name: "Free"}}} = conn, %{
        "user" => %{"bigquery_project_id" => _id}
      }) do
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
    with {:ok, _user} <- Users.delete_user(user),
         {:ok, _response} <- Stripe.delete_customer(stripe_customer) do
      conn
      |> configure_session(drop: true)
      |> redirect(to: ~p"/auth/login?#{%{user_deleted: true}}")
    else
      _err ->
        conn
        |> put_flash(
          :error,
          "Something went wrong! Please try again and contact support if this continues."
        )
        |> render("edit.html")
    end
  end

  def delete(%{assigns: %{user: user}} = conn, _params) do
    case Users.delete_user(user) do
      {:ok, _user} ->
        conn
        |> configure_session(drop: true)
        |> redirect(to: ~p"/auth/login?#{%{user_deleted: true}}")

      _err ->
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

        conn
        |> put_flash(:info, "API key restored!")
        |> redirect(to: ~p"/dashboard")

      nil ->
        %{assigns: %{user: user}} = conn
        new_api_key = :crypto.strong_rand_bytes(12) |> Base.url_encode64() |> binary_part(0, 12)
        old_api_key = user.api_key
        auth_params = %{api_key: new_api_key, old_api_key: old_api_key}

        Users.update_user_all_fields(user, auth_params)

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
      conn
      |> put_flash(:error, "Not authorized to transfer ownership to this team member")
      |> redirect(to: Routes.user_path(conn, :edit) <> "#change-account-owner")
    else
      with {:ok, new_owner} <- Users.change_owner(team_user, user),
           {:ok, _resp} <- TeamUsers.delete_team_user(team_user) do
        if user.billing_account do
          Stripe.update_customer(user.billing_account.stripe_customer, %{email: new_owner.email})
        end

        conn
        |> put_flash(:info, "Owner successfully changed!")
        |> redirect(to: Routes.user_path(conn, :edit) <> "#team-members")
      else
        {:error, changeset} ->
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
          conn
          |> put_flash(:error, "Something went wrong. Please contact support if this continues.")
          |> redirect(to: Routes.user_path(conn, :edit) <> "#change-account-owner")
      end
    end
  end
end
