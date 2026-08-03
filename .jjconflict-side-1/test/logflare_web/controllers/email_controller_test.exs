defmodule LogflareWeb.EmailControllerTest do
  use LogflareWeb.ConnCase

  alias Logflare.Auth
  alias Logflare.Mailer

  setup do
    insert(:plan)
    :ok
  end

  describe "GET /auth/login/email" do
    test "renders the email login form", %{conn: conn} do
      conn = get(conn, ~p"/auth/login/email")

      assert html_response(conn, 200) =~ "email"
      assert conn.assigns.flash["error"] == nil
    end
  end

  describe "POST /auth/login/email (send_link)" do
    test "sends email and redirects to verify page", %{conn: conn} do
      expect(Mailer, :deliver, fn _email -> {:ok, %{}} end)

      conn = post(conn, ~p"/auth/login/email", %{email: "some-email@logflare.app"})

      assert redirected_to(conn) =~ "/auth/login/email/verify"
      assert conn.assigns.flash["info"] =~ "Check your email"
    end

    test "trims and downcases email", %{conn: conn} do
      expect(Mailer, :deliver, fn email ->
        [{_, to_email}] = email.to
        assert to_email == "test@logflare.app"
        {:ok, %{}}
      end)

      conn = post(conn, ~p"/auth/login/email", %{email: "  Test@Logflare.app  "})

      assert redirected_to(conn) =~ "/auth/login/email/verify"
    end

    test "sends no-link email when vercel_setup session is set", %{conn: conn} do
      expect(Mailer, :deliver, fn email ->
        refute email.text_body =~ "Sign in to Logflare with this link"
        assert email.text_body =~ "Copy and paste this token"
        {:ok, %{}}
      end)

      conn =
        conn
        |> Plug.Test.init_test_session(%{vercel_setup: true})
        |> post(~p"/auth/login/email", %{email: "vercel@logflare.app"})

      assert redirected_to(conn) =~ "/auth/login/email/verify"
    end

    test "renders error when email is empty", %{conn: conn} do
      conn = post(conn, ~p"/auth/login/email", %{email: ""})

      assert html_response(conn, 200)
      assert conn.assigns.flash["error"] =~ "cannot be empty"
    end

    test "renders error when email param is missing", %{conn: conn} do
      conn = post(conn, ~p"/auth/login/email", %{})

      assert html_response(conn, 200)
      assert conn.assigns.flash["error"] =~ "cannot be empty"
    end
  end

  describe "GET /auth/login/email/verify" do
    test "renders the verify token form", %{conn: conn} do
      conn = get(conn, ~p"/auth/login/email/verify")

      assert html_response(conn, 200)
    end
  end

  describe "POST /auth/login/email/verify (verify_token_form)" do
    test "with valid token signs in user", %{conn: conn} do
      email = "valid@logflare.app"
      token = Auth.gen_email_token(email)

      conn = post(conn, ~p"/auth/login/email/verify", %{token: token})

      assert redirected_to(conn) =~ "/"
    end

    test "with expired token redirects with error", %{conn: conn} do
      expect(Phoenix.Token, :verify, fn _endpoint, _salt, _token, _opts -> {:error, :expired} end)

      conn = post(conn, ~p"/auth/login/email/verify", %{token: "expired-token"})

      assert redirected_to(conn) =~ "/auth/login"
      assert conn.assigns.flash["error"] =~ "expired"
    end

    test "with invalid token redirects with error", %{conn: conn} do
      conn = post(conn, ~p"/auth/login/email/verify", %{token: "not-a-valid-token"})

      assert redirected_to(conn) =~ "/auth/login"
      assert conn.assigns.flash["error"] =~ "error signing in"
    end
  end

  describe "GET /auth/email/callback/:token" do
    test "with valid token signs in user", %{conn: conn} do
      email = "callback@logflare.app"
      token = Auth.gen_email_token(email)

      conn = get(conn, ~p"/auth/email/callback/#{token}")

      assert redirected_to(conn) =~ "/"
    end

    test "with expired token redirects with error", %{conn: conn} do
      expect(Phoenix.Token, :verify, fn _endpoint, _salt, _token, _opts -> {:error, :expired} end)

      conn = get(conn, ~p"/auth/email/callback/expired-token")

      assert redirected_to(conn) =~ "/auth/login"
      assert conn.assigns.flash["error"] =~ "expired"
    end

    test "with invalid token redirects with error", %{conn: conn} do
      conn = get(conn, ~p"/auth/email/callback/not-a-valid-token")

      assert redirected_to(conn) =~ "/auth/login"
      assert conn.assigns.flash["error"] =~ "error signing in"
    end
  end
end
