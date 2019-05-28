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
    old_user = conn.assigns.user
    changeset = User.update_by_user_changeset(old_user, params)

    case Repo.update(changeset) do
      {:ok, _user} ->
        Users.Cache.delete_cache_key_by_id(old_user.id)

        case params do
          %{"bigquery_project_id" => _project_id} ->
            SourceManager.reset_all_user_tables(old_user)

          _ ->
            nil
        end

        conn
        |> put_flash(:info, "Account updated!")
        |> redirect(to: Routes.user_path(conn, :edit))

      {:error, changeset} ->
        conn
        |> put_flash(:error, "Something went wrong!")
        |> render("edit.html",
          changeset: changeset,
          user: old_user,
          service_account: @service_account
        )
    end
  end

  def delete(conn, _params) do
    user_id = conn.assigns.user.id
    Repo.get!(User, user_id) |> Repo.delete!()
    BigQuery.delete_dataset(user_id)
    CloudResourceManager.set_iam_policy!()

    conn
    |> put_flash(:info, "Account deleted!")
    |> redirect(to: Routes.marketing_path(conn, :index))
  end
end
