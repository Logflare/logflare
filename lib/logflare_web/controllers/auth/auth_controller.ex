defmodule LogflareWeb.AuthController do
  use LogflareWeb, :controller

  alias Logflare.{Users, TeamUsers, Teams}
  alias Logflare.Auth
  alias Logflare.AccountEmail
  alias Logflare.Mailer
  alias Logflare.Google.CloudResourceManager
  alias Logflare.Google.BigQuery
  alias Logflare.Vercel

  @max_age 86_400

  def logout(conn, _params) do
    conn
    |> configure_session(drop: true)
    |> redirect(to: Routes.auth_path(conn, :login))
  end

  def login(conn, _params) do
    last_provider = conn.cookies["_logflare_last_provider"]

    user = %{provider: last_provider}

    conn
    |> maybe_flash_invite_message()
    |> maybe_flash_account_deleted()
    |> render("login.html", last_login: user)
  end

  def check_invite_token_and_signin(conn, auth_params) do
    case get_session(conn, :invite_token) do
      nil ->
        conn
        |> signin(auth_params)

      invite_token ->
        case Auth.verify_token(invite_token, @max_age) do
          {:ok, invited_by_team_id} ->
            conn
            |> invited_signin(auth_params, invited_by_team_id)

          {:error, :expired} ->
            conn
            |> put_flash(:error, "That invite link is expired!")
            |> put_session(:invite_token, nil)
            |> redirect(to: Routes.auth_path(conn, :login))

          {:error, _reason} ->
            conn
            |> put_flash(
              :error,
              "There is an issue with this invite link. Get a new invite link and try again!"
            )
            |> put_session(:invite_token, nil)
            |> redirect(to: Routes.auth_path(conn, :login))
        end
    end
  end

  def invited_signin(conn, auth_params, invited_by_team_id) do
    team = Teams.get_team!(invited_by_team_id) |> Teams.preload_user()
    invited_by_user = team.user
    invitee_exists_as_owner? = invited_by_user.email == auth_params.email

    if invitee_exists_as_owner? do
      conn
      |> put_flash(:error, "You are already the owner for this account!")
      |> put_session(:invite_token, nil)
      |> signin(auth_params)
    else
      signin_invitee(conn, auth_params, team)
    end
  end

  def signin_invitee(conn, auth_params, team) do
    case TeamUsers.insert_or_update_team_user(team, auth_params) do
      {:ok, team_user} ->
        CloudResourceManager.set_iam_policy()
        BigQuery.patch_dataset_access(team.user)

        conn
        |> put_flash(:info, "Welcome to Logflare!")
        |> put_session(:user_id, team.user.id)
        |> put_session(:team_user_id, team_user.id)
        |> put_session(:invite_token, nil)
        |> redirect(to: Routes.source_path(conn, :dashboard))

      {:error, :limit_reached} ->
        conn
        |> put_flash(
          :error,
          "Team member limit reached. Please contact the account owner for support."
        )
        |> put_session(:invite_token, nil)
        |> redirect(to: Routes.auth_path(conn, :login))

      {:error, _changeset} ->
        conn
        |> put_flash(
          :error,
          "There was an issue siging into this team. If this continues please contact support."
        )
        |> put_session(:invite_token, nil)
        |> redirect(to: Routes.auth_path(conn, :login))
    end
  end

  def create_and_sign_in(%{assigns: %{team_user: team_user}} = conn, _params) do
    {:ok, user} =
      team_user
      |> Map.take([:email, :email_preferred, :provider, :image, :name, :provider_uid, :token])
      |> Users.insert_user()

    auth_params =
      Map.take(user, [:email, :email_preferred, :provider, :image, :name, :provider_uid, :token])

    signin(conn, auth_params)
  end

  def signin(conn, auth_params) do
    team_users = TeamUsers.list_team_users_by_and_preload(email: auth_params.email)
    user = Users.get_user_by(email: auth_params.email)

    cond do
      !Enum.empty?(team_users) and is_nil(user) ->
        team_user = hd(team_users)
        user = Users.get_user(team_user.team.user_id)

        case TeamUsers.insert_or_update_team_user(team_user.team, auth_params) do
          {:ok, team_user} ->
            CloudResourceManager.set_iam_policy()
            BigQuery.patch_dataset_access(user)

            conn
            |> put_flash(:info, "Welcome back!")
            |> put_session(:user_id, user.id)
            |> put_session(:team_user_id, team_user.id)
            |> redirect(to: Routes.source_path(conn, :dashboard))

          {:error, _} ->
            conn
            |> put_flash(
              :error,
              "There was an error signing in. Please contact support if this continues."
            )
            |> redirect(to: Routes.auth_path(conn, :login))
        end

      true ->
        case Users.insert_or_update_user(auth_params) do
          {:ok, user} ->
            AccountEmail.welcome(user) |> Mailer.deliver()
            CloudResourceManager.set_iam_policy()
            BigQuery.patch_dataset_access(user)

            conn
            |> put_flash(:info, "Thanks for signing up! Now create a source!")
            |> put_session(:user_id, user.id)
            |> redirect(to: Routes.source_path(conn, :new, signup: true))

          {:ok_found_user, user} ->
            CloudResourceManager.set_iam_policy()
            BigQuery.patch_dataset_access(user)

            oauth_params = get_session(conn, :oauth_params)
            vercel_setup_params = get_session(conn, :vercel_setup)

            cond do
              oauth_params ->
                conn
                |> redirect_for_oauth(user)

              vercel_setup_params ->
                auth_params = vercel_setup_params["auth_params"]
                install_id = auth_params["installation_id"]

                {:ok, _auth} =
                  Vercel.find_by_or_create_auth([installation_id: install_id], user, auth_params)

                conn
                |> put_session(:vercel_setup, nil)
                |> redirect(external: vercel_setup_params["next"])

              true ->
                conn
                |> put_flash(:info, "Welcome back!")
                |> put_session(:user_id, user.id)
                |> maybe_redirect_team_user()
            end

          {:error, _reason} ->
            conn
            |> put_flash(:error, "Error signing in.")
            |> redirect(to: Routes.auth_path(conn, :login))
        end
    end
  end

  def redirect_for_vercel(conn, user) do
    vercel_setup_params = get_session(conn, :vercel_setup)
    auth_params = vercel_setup_params["auth_params"]
    install_id = auth_params["installation_id"]

    {:ok, _auth} = Vercel.find_by_or_create_auth([installation_id: install_id], user, auth_params)

    conn
    |> put_session(:vercel_setup, nil)
    |> redirect(external: vercel_setup_params["next"])
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

  defp maybe_redirect_team_user(conn) do
    team_user_id = conn.cookies["_logflare_team_user_id"]
    user_id = conn.cookies["_logflare_user_id"]

    if team_user_id && user_id do
      redirect(conn,
        to:
          Routes.team_user_path(conn, :change_team, %{
            "user_id" => user_id,
            "team_user_id" => team_user_id
          })
      )
    else
      redirect(conn, to: Routes.source_path(conn, :dashboard))
    end
  end

  defp maybe_flash_account_deleted(conn) do
    cond do
      conn.params["user_deleted"] ->
        put_flash(conn, :info, "Your account has been deleted!")

      conn.params["team_user_deleted"] ->
        put_flash(conn, :info, "Your member profile has been deleted!")

      true ->
        conn
    end
  end

  defp maybe_flash_invite_message(conn) do
    cond do
      invite_token = conn.params["invite_token"] ->
        conn
        |> put_session(:invite_token, invite_token)
        |> put_flash(:info, "You've been invited to sign into Logflare!")

      get_session(conn, :invite_token) ->
        conn
        |> put_flash(:info, "You've been invited to sign into Logflare!")

      true ->
        conn
    end
  end
end
