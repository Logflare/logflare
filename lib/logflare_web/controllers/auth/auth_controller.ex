defmodule LogflareWeb.AuthController do
  use LogflareWeb, :controller

  alias Logflare.Users
  alias Logflare.AccountEmail
  alias Logflare.Mailer
  alias Logflare.Google.CloudResourceManager
  alias Logflare.Google.BigQuery

  def logout(conn, _params) do
    conn
    |> configure_session(drop: true)
    |> redirect(to: Routes.marketing_path(conn, :index))
  end

  def login(conn, _params) do
    render(conn, "login.html")
  end

  def signin(conn, auth_params) do
    oauth_params = get_session(conn, :oauth_params)

    case Users.insert_or_update_user(auth_params) do
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
end
