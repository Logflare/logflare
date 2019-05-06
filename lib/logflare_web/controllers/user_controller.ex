defmodule LogflareWeb.UserController do
  use LogflareWeb, :controller

  alias Logflare.Repo
  alias Logflare.User
  alias Logflare.AccountCache
  alias Logflare.Google.BigQuery
  alias Logflare.Google.CloudResourceManager

  def edit(conn, _params) do
    user = conn.assigns.user
    changeset = User.changeset(user, %{})

    render(conn, "edit.html", changeset: changeset, user: user)
  end

  def update(conn, %{"user" => user}) do
    old_user = conn.assigns.user
    changeset = User.changeset(old_user, user)

    case Repo.update(changeset) do
      {:ok, _user} ->
        conn
        |> put_flash(:info, "Account updated!")
        |> redirect(to: Routes.user_path(conn, :edit))

      {:error, changeset} ->
        conn
        |> put_flash(:error, "Something went wrong!")
        |> render("edit.html", changeset: changeset, user: old_user)
    end
  end

  def delete(conn, _params) do
    user_id = conn.assigns.user.id
    Repo.get!(User, user_id) |> Repo.delete!()
    AccountCache.remove_account(conn.assigns.user.api_key)
    BigQuery.delete_dataset(user_id)
    CloudResourceManager.set_iam_policy!()

    conn
    |> put_flash(:info, "Account deleted!")
    |> redirect(to: Routes.marketing_path(conn, :index))
  end
end
