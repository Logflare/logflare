defmodule LogflareWeb.AuthController do
  use LogflareWeb, :controller
  use Phoenix.HTML

  plug(Ueberauth)

  alias Logflare.User
  alias Logflare.Repo

  def callback(%{assigns: %{ueberauth_auth: auth}} = conn, _params) do
    api_key = :crypto.strong_rand_bytes(12) |> Base.url_encode64() |> binary_part(0, 12)

    user_params = %{
      token: auth.credentials.token,
      email: auth.info.email,
      provider: "github",
      api_key: api_key
    }

    changeset = User.changeset(%User{}, user_params)

    conn
    |> signin(changeset)
  end

  def logout(conn, _params) do
    conn
    |> configure_session(drop: true)
    |> redirect(to: Routes.source_path(conn, :index))
  end

  def new_api_key(conn, _params) do
    case conn.params["undo"] do
      "true" ->
        %{assigns: %{user: user}} = conn
        new_api_key = user.old_api_key
        old_api_key = user.api_key
        user_params = %{api_key: new_api_key, old_api_key: old_api_key}

        changeset = User.changeset(user, user_params)
        Repo.update(changeset)

        conn
        |> put_flash(:info, "API key restored!")
        |> redirect(to: Routes.source_path(conn, :dashboard))

      nil ->
        %{assigns: %{user: user}} = conn
        new_api_key = :crypto.strong_rand_bytes(12) |> Base.url_encode64() |> binary_part(0, 12)
        old_api_key = user.api_key
        user_params = %{api_key: new_api_key, old_api_key: old_api_key}

        changeset = User.changeset(user, user_params)
        Repo.update(changeset)

        conn
        |> put_flash(:info, [
          "API key reset! ",
          link("Undo?", to: Routes.auth_path(conn, :new_api_key, undo: true))
        ])
        |> redirect(to: Routes.source_path(conn, :dashboard))
    end
  end

  defp signin(conn, changeset) do
    oauth_params = get_session(conn, :oauth_params)

    case insert_or_update_user(changeset) do
      {:ok, user} ->
        conn
        |> put_flash(:info, "Thanks for signing up! Now create a source!")
        |> put_session(:user_id, user.id)
        |> redirect(to: Routes.source_path(conn, :new, signup: true))

      {:ok_found_user, user} ->
        case is_nil(oauth_params) do
          true ->
            conn
            |> put_flash(:info, "Welcome back!")
            |> put_session(:user_id, user.id)
            |> redirect(to: Routes.source_path(conn, :dashboard))

          false ->
            conn
            |> redirect_for_oauth(user)
        end

      {:error, _reason} ->
        conn
        |> put_flash(:error, "Error signing in.")
        |> redirect(to: Routes.source_path(conn, :index))
    end
  end

  def redirect_for_oauth(conn, user) do
    oauth_params = get_session(conn, :oauth_params)

    conn
    |> put_session(:user_id, user.id)
    |> put_session(:oauth_params, nil)
    |> redirect(
      to:
        Routes.oauth_authorization_path(conn, :new,
          client_id: oauth_params["client_id"],
          redirect_uri: oauth_params["redirect_uri"],
          response_type: oauth_params["response_type"],
          scope: oauth_params["scope"]
        )
    )
  end

  defp insert_or_update_user(changeset) do
    case Repo.get_by(User, email: changeset.changes.email) do
      nil ->
        Repo.insert(changeset)

      user ->
        {:ok_found_user, user}
    end
  end
end
