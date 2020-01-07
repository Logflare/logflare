defmodule LogflareWeb.AuthController do
  use LogflareWeb, :controller

  alias Logflare.{Users, TeamUsers, Teams}
  alias Logflare.Auth
  alias Logflare.AccountEmail
  alias Logflare.Mailer
  alias Logflare.Google.CloudResourceManager
  alias Logflare.Google.BigQuery

  def logout(conn, _params) do
    conn
    |> configure_session(drop: true)
    |> redirect(to: Routes.marketing_path(conn, :index))
  end

  def login(conn, %{"invite_token" => invite_token} = params) do
    put_session(conn, :invite_token, invite_token)
    |> put_flash(:info, "You've been invited to sign into Logflare!")
    |> render("login.html")
  end

  def login(conn, _params) do
    if get_session(conn, :invite_token) do
      put_flash(conn, :info, "You've been invited to sign into Logflare!")
      |> render("login.html")
    else
      render(conn, "login.html")
    end
  end

  def invited_signin(conn, auth_params, invite_token) do
    {:ok, invited_by_team_id} = Auth.verify_token(invite_token)
    team = Teams.get_team!(invited_by_team_id) |> Teams.preload_user()
    invited_by_user = team.user
    invitee_exists_as_owner? = invited_by_user.provider_uid == auth_params.provider_uid

    if invitee_exists_as_owner? do
      conn
      |> put_flash(:error, "Invite error. You are already the owner for this account.")
      |> put_session(:invite_token, nil)
      |> signin(auth_params)
    else
      signin_invitee(conn, auth_params, invite_token)
    end
  end

  def signin_invitee(conn, auth_params, invite_token) do
    {:ok, invited_by_team_id} = Auth.verify_token(invite_token)
    invited_by_user_id = Teams.get_team!(invited_by_team_id).user_id

    case TeamUsers.get_team_user_by_team_provider_uid(
           invited_by_team_id,
           auth_params.provider_uid
         ) do
      nil ->
        {:ok, team_user} = TeamUsers.create_team_user(invited_by_team_id, auth_params)

        conn
        |> put_flash(:error, "Thanks for joining this Logflare team!")
        |> put_session(:user_id, invited_by_user_id)
        |> put_session(:team_user_id, team_user.id)
        |> put_session(:invite_token, nil)
        |> redirect(to: Routes.source_path(conn, :dashboard))

      team_user ->
        conn
        |> put_flash(:info, "Welcome back!")
        |> put_session(:user_id, invited_by_user_id)
        |> put_session(:team_user_id, team_user.id)
        |> put_session(:invite_token, nil)
        |> redirect(to: Routes.source_path(conn, :dashboard))
    end
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
