defmodule LogflareWeb.UserController do
  use LogflareWeb, :controller

  alias Logflare.Repo
  alias Logflare.User
  alias Logflare.Users
  alias Logflare.Google.BigQuery
  alias Logflare.Google.CloudResourceManager
  alias Logflare.SourceManager

  @service_account Application.get_env(:logflare, Logflare.Google)[:service_account]

  def edit(conn, _params) do
    user = conn.assigns.user
    changeset = User.update_by_user_changeset(user, %{})

    render(conn, "edit.html", changeset: changeset, user: user, service_account: @service_account)
  end

  def update(conn, %{"user" => params}) do
    conn.assigns.user
    |> User.update_by_user_changeset(params)
    |> Repo.update()
    |> case do
      {:ok, user} ->
        Users.Cache.delete_cache_key_by_id(user.id)

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

  def delete(conn, _params) do
    user_id = conn.assigns.user.id
    Repo.get!(User, user_id) |> Repo.delete!()
    BigQuery.delete_dataset(user_id)
    CloudResourceManager.set_iam_policy()

    conn
    |> put_flash(:info, "Account deleted!")
    |> redirect(to: Routes.marketing_path(conn, :index))
  end
end
