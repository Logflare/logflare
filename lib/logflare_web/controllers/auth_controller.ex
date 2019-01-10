defmodule LogflareWeb.AuthController do
  use LogflareWeb, :controller

  plug Ueberauth

  alias Logflare.User
  alias Logflare.Repo

  def callback(%{assigns: %{ueberauth_auth: auth}} = conn, _params) do
      api_key = :crypto.strong_rand_bytes(12) |> Base.url_encode64 |> binary_part(0, 12)
      user_params = %{token: auth.credentials.token, email: auth.info.email, provider: "github", api_key: api_key}
      changeset = User.changeset(%User{}, user_params)

      signin(conn, changeset)
  end

  def logout(conn, _params) do
      conn
      |> configure_session(drop: true)
      |> redirect(to: source_path(conn, :index))
  end

  def new_api_key(conn, _params) do
    %{assigns: %{user: user}} = conn
    api_key = :crypto.strong_rand_bytes(12) |> Base.url_encode64 |> binary_part(0, 12)
    user_params = %{api_key: api_key}
    changeset = User.changeset(user, user_params)

    Repo.update(changeset)

    conn
    |> put_flash(:info, "API key reset!")
    |> redirect(to: source_path(conn, :dashboard))
  end

  defp signin(conn, changeset) do
    case insert_or_update_user(changeset) do
      {:ok, user} ->
        conn
        |> put_flash(:info, "Welcome back!")
        |> put_session(:user_id, user.id)
        |> redirect(to: source_path(conn, :dashboard))
      {:error, _reason} ->
        conn
        |> put_flash(:error, "Error signing in.")
        |> redirect(to: source_path(conn, :index))
    end
  end

  defp insert_or_update_user(changeset) do
    case Repo.get_by(User, email: changeset.changes.email) do
      nil ->
        Repo.insert(changeset)
      user ->
        {:ok, user}
    end
  end

end
