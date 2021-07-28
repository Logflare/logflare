defmodule LogflareWeb.Auth.EmailController do
  use LogflareWeb, :controller

  alias Logflare.Auth
  alias Logflare.Mailer
  alias LogflareWeb.AuthController

  # 30 minutes
  @max_age 1_800

  def login(conn, _params) do
    render(conn, "email_login.html")
  end

  def verify_token(conn, _params) do
    render(conn, "verify_token.html")
  end

  def send_link(conn, %{"email" => email}) do
    email = email |> String.downcase() |> String.trim()

    if get_session(conn, :vercel_setup) do
      Auth.Email.auth_email_no_link(email)
    else
      Auth.Email.auth_email(email)
    end
    |> Mailer.deliver()

    conn
    |> put_flash(:info, "Check your email for a sign in link!")
    |> redirect(to: Routes.email_path(conn, :verify_token))
  end

  def callback(conn, %{"token" => token}) do
    token = String.trim(token)

    case Auth.verify_token(token, @max_age) do
      {:ok, email} ->
        auth_params = %{
          token: token,
          email: email,
          email_preferred: email,
          provider: "email",
          provider_uid: email,
          image: Auth.gravatar_link(email)
        }

        AuthController.check_invite_token_and_signin(conn, auth_params)

      {:error, :expired} ->
        conn
        |> put_flash(
          :error,
          "That link is expired. Please sign in again."
        )
        |> redirect(to: Routes.auth_path(conn, :login))

      {:error, _reason} ->
        conn
        |> put_flash(
          :error,
          "There was an error signing in. Please try again or contact support if this continues."
        )
        |> redirect(to: Routes.auth_path(conn, :login))
    end
  end
end
