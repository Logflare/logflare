defmodule LogflareWeb.UserController do
  use LogflareWeb, :controller
  use Phoenix.HTML

  plug LogflareWeb.Plugs.AuthMustBeOwner

  alias Logflare.{User, Repo, Users, TeamUsers, Source.Supervisor, BillingAccounts.Stripe}

  @service_account Application.get_env(:logflare, Logflare.Google)[:service_account] || ""

  def api_show(%{assigns: %{user: user}} = conn, _params) do
    conn
    |> json(user)
  end

  def edit(%{assigns: %{user: user}} = conn, _params) do
    changeset = User.user_allowed_changeset(user, %{})

    render(conn, "edit.html",
      changeset: changeset,
      user: user,
      service_account: @service_account
    )
  end

  def update(%{assigns: %{plan: %{name: "Free"}}} = conn, %{
        "user" => %{"bigquery_project_id" => _id}
      }) do
    message = [
      "Please ",
      Phoenix.HTML.Link.link("upgrade to a paid plan",
        to: "#{Routes.billing_account_path(conn, :edit)}"
      ),
      " to setup your own BigQuery backend."
    ]

    conn
    |> put_flash(:error, message)
    |> redirect(to: Routes.user_path(conn, :edit))
  end

  def update(%{assigns: %{user: user}} = conn, %{"user" => params}) do
    user
    |> User.user_allowed_changeset(params)
    |> Repo.update()
    |> case do
      {:ok, updated_user} ->
        if updated_user.bigquery_project_id != user.bigquery_project_id,
          do: Supervisor.reset_all_user_sources(user)

        conn
        |> put_flash(:info, "Account updated!")
        |> redirect(to: Routes.user_path(conn, :edit))

      {:error, changeset} ->
        conn
        |> put_flash(:error, "Something went wrong! See below for errors.")
        |> put_status(406)
        |> render("edit.html",
          changeset: changeset,
          user: conn.assigns.user,
          service_account: @service_account
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
      |> redirect(to: Routes.auth_path(conn, :login, user_deleted: true))
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
    with {:ok, _user} <- Users.delete_user(user) do
      conn
      |> configure_session(drop: true)
      |> redirect(to: Routes.auth_path(conn, :login, user_deleted: true))
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

  def new_api_key(conn, _params) do
    case conn.params["undo"] do
      "true" ->
        %{assigns: %{user: user}} = conn
        new_api_key = user.old_api_key
        old_api_key = user.api_key
        auth_params = %{api_key: new_api_key, old_api_key: old_api_key}

        changeset = User.changeset(user, auth_params)
        Repo.update(changeset)

        conn
        |> put_flash(:info, "API key restored!")
        |> redirect(to: Routes.source_path(conn, :dashboard))

      nil ->
        %{assigns: %{user: user}} = conn
        new_api_key = :crypto.strong_rand_bytes(12) |> Base.url_encode64() |> binary_part(0, 12)
        old_api_key = user.api_key
        auth_params = %{api_key: new_api_key, old_api_key: old_api_key}

        changeset = User.changeset(user, auth_params)
        Repo.update(changeset)

        conn
        |> put_flash(:info, [
          "API key reset! ",
          link("Undo?", to: Routes.user_path(conn, :new_api_key, undo: true))
        ])
        |> redirect(to: Routes.source_path(conn, :dashboard))
    end
  end

  def change_owner(%{assigns: %{user: _user}} = conn, %{"user" => %{"team_user_id" => ""}}) do
    conn
    |> put_flash(:error, "Please select a team member!")
    |> redirect(to: Routes.user_path(conn, :edit) <> "#change-account-owner")
  end

  def change_owner(%{assigns: %{user: user}} = conn, %{"user" => %{"team_user_id" => id}}) do
    with team_user <- TeamUsers.get_team_user(id),
         {:ok, _user} <- Users.change_owner(team_user, user),
         {:ok, _resp} <- TeamUsers.delete_team_user(team_user) do
      conn
      |> put_flash(:info, "Owner successfully changed!")
      |> redirect(to: Routes.user_path(conn, :edit) <> "#team-members")
    else
      _err ->
        conn
        |> put_flash(:error, "Something went wrong. Please contact support if this continues.")
        |> redirect(to: Routes.user_path(conn, :edit) <> "#change-account-owner")
    end
  end
end
