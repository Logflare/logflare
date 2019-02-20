defmodule LogflareWeb.UserController do
  use LogflareWeb, :controller

  alias Logflare.Repo
  alias Logflare.User

  def edit(conn, _params) do
    user_id = conn.assigns.user.id
    user = Repo.get(User, user_id)
    changeset = User.changeset(user, %{})

    render(conn, "edit.html", changeset: changeset, user: user)
  end

  def update(conn, %{"user" => user}) do
    user_id = conn.assigns.user.id
    old_user = Repo.get(User, user_id)
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
end
