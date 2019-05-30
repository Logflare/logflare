defmodule LogflareWeb.AuthController do
  use LogflareWeb, :controller
  use Phoenix.HTML

  plug Ueberauth

  alias Logflare.User
  alias Logflare.Repo
  alias Logflare.AccountEmail
  alias Logflare.Mailer
  alias Logflare.Source
  alias Logflare.Google.CloudResourceManager
  alias Logflare.Google.BigQuery

  @salt Application.get_env(:logflare, LogflareWeb.Endpoint)[:secret_key_base]
  @max_age 86_400

  def callback(%{assigns: %{ueberauth_auth: auth}} = conn, _params) do
    api_key = :crypto.strong_rand_bytes(12) |> Base.url_encode64() |> binary_part(0, 12)

    user_params = %{
      token: auth.credentials.token,
      email: auth.info.email,
      email_preferred: auth.info.email,
      provider: Atom.to_string(auth.provider),
      api_key: api_key,
      image: auth.info.image,
      name: auth.info.name
    }

    changeset = User.changeset(%User{}, user_params)

    conn
    |> signin(changeset)
  end

  def logout(conn, _params) do
    conn
    |> configure_session(drop: true)
    |> redirect(to: Routes.marketing_path(conn, :index))
  end

  def login(conn, _params) do
    render(conn, "login.html")
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
        AccountEmail.welcome(user) |> Mailer.deliver()
        CloudResourceManager.set_iam_policy()

        conn
        |> put_flash(:info, "Thanks for signing up! Now create a source!")
        |> put_session(:user_id, user.id)
        |> redirect(to: Routes.source_path(conn, :new, signup: true))

      {:ok_found_user, user} ->
        CloudResourceManager.set_iam_policy()
        BigQuery.patch_dataset_access!(user.id)

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
        |> redirect(to: Routes.marketing_path(conn, :index))
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

  def unsubscribe(conn, %{"id" => source_id, "token" => token}) do
    source = Repo.get(Source, source_id)
    source_changes = %{user_email_notifications: false}
    changeset = Source.update_by_user_changeset(source, source_changes)

    case verify_token(token) do
      {:ok, _email} ->
        case Repo.update(changeset) do
          {:ok, _source} ->
            conn
            |> put_flash(:info, "Unsubscribed!")
            |> redirect(to: Routes.marketing_path(conn, :index))

          {:error, _changeset} ->
            conn
            |> put_flash(:error, "Something went wrong!")
            |> redirect(to: Routes.marketing_path(conn, :index))
        end

      {:error, :expired} ->
        conn
        |> put_flash(:error, "That link is expired!")
        |> redirect(to: Routes.marketing_path(conn, :index))

      {:error, :invalid} ->
        conn
        |> put_flash(:error, "Bad link!")
        |> redirect(to: Routes.marketing_path(conn, :index))
    end
  end

  def unsubscribe_stranger(conn, %{"id" => source_id, "token" => token}) do
    case verify_token(token) do
      {:ok, email} ->
        source = Repo.get(Source, source_id)

        source_changes = %{
          other_email_notifications: filter_email(email, source.other_email_notifications)
        }

        changeset = Source.update_by_user_changeset(source, source_changes)

        case Repo.update(changeset) do
          {:ok, _source} ->
            conn
            |> put_flash(:info, "Unsubscribed!")
            |> redirect(to: Routes.marketing_path(conn, :index))

          {:error, _changeset} ->
            conn
            |> put_flash(:error, "Something went wrong!")
            |> redirect(to: Routes.marketing_path(conn, :index))
        end

      {:error, :expired} ->
        conn
        |> put_flash(:error, "That link is expired!")
        |> redirect(to: Routes.marketing_path(conn, :index))

      {:error, :invalid} ->
        conn
        |> put_flash(:error, "Bad link!")
        |> redirect(to: Routes.marketing_path(conn, :index))
    end
  end

  defp filter_email(email, other_emails) do
    String.split(other_emails, ",")
    |> Enum.map(fn e -> String.trim(e) end)
    |> Enum.filter(fn e -> e != email end)
    |> Enum.join(", ")
  end

  defp insert_or_update_user(changeset) do
    case Repo.get_by(User, email: changeset.changes.email) do
      nil ->
        Repo.insert(changeset)

      user ->
        updated_params = %{
          token: changeset.changes.token,
          provider: changeset.changes.provider,
          image: changeset.changes.image
        }

        updated_changeset = User.changeset(user, updated_params)

        Repo.update(updated_changeset)
        updated_user = Repo.get_by(User, email: changeset.changes.email)
        {:ok_found_user, updated_user}
    end
  end

  defp verify_token(token),
    do: Phoenix.Token.verify(LogflareWeb.Endpoint, @salt, token, max_age: @max_age)
end
