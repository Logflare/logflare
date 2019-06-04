defmodule LogflareWeb.UserController do
  use LogflareWeb, :controller

  alias Logflare.{User, Users, Repo}
  alias Logflare.Google.BigQuery
  alias Logflare.Google.CloudResourceManager
  alias Logflare.SourceManager

  @service_account Application.get_env(:logflare, Logflare.Google)[:service_account] || ""

  def edit(%{assigns: %{user: user}} = conn, _params) do
    changeset = User.update_by_user_changeset(user, %{})
    render(conn, "edit.html", changeset: changeset, user: user, service_account: @service_account)
  end

  def update(conn, %{"user" => params}) do
    conn.assigns.user
    |> User.update_by_user_changeset(params)
    |> Repo.update()
    |> case do
      {:ok, user} ->

        conn
        |> put_flash(:info, "Account updated!")
        |> redirect(to: Routes.user_path(conn, :edit))

      {:error, changeset} ->
        conn
        |> put_flash(:error, "Something went wrong!")
        |> render("edit.html",
          changeset: changeset,
          user: conn.assigns.user,
          service_account: @service_account
        )
    end
  end

  def delete(%{assigns: %{user: user}} = conn, _params) do
    # TODO: soft delete, delayed deleted
    Repo.delete!(user)
    BigQuery.delete_dataset(user.id)

    spawn(fn ->
      CloudResourceManager.set_iam_policy()
    end)

    conn
    |> put_flash(:info, "Account deleted!")
    |> redirect(to: Routes.marketing_path(conn, :index))
  end
end
