defmodule LogflareWeb.AuthControllerTest do
  use LogflareWeb.ConnCase, async: true
  use Mimic

  alias Logflare.Auth
  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.SingleTenant

  setup do
    insert(:plan)

    stub(BigQueryAdaptor, :update_iam_policy, fn -> :ok end)
    stub(BigQueryAdaptor, :update_iam_policy, fn _user -> :ok end)
    stub(BigQueryAdaptor, :patch_dataset_access, fn _user -> {:ok, :patch_attempted} end)

    :ok
  end

  describe "GET /auth/logout" do
    test "clears session and redirects to login", %{conn: conn} do
      user = insert(:user)

      conn =
        conn
        |> login_user(user)
        |> put_session(:some_data, "test")
        |> get("/auth/logout")

      assert redirected_to(conn, 302) == ~p"/auth/login"
      assert conn.resp_cookies["_logflare_user_id"].value == ""
      assert conn.resp_cookies["_logflare_team_user_id"].value == ""
      assert conn.resp_cookies["_logflare_last_provider"].value == ""
    end
  end

  describe "GET /auth/login" do
    test "renders login page", %{conn: conn} do
      conn = get(conn, "/auth/login")
      assert html_response(conn, 200) =~ "login"
    end

    test "renders login page with user_deleted flash", %{conn: conn} do
      conn = get(conn, "/auth/login", user_deleted: "true")
      assert html_response(conn, 200)
      assert Phoenix.Flash.get(conn.assigns.flash, :info) =~ "account has been deleted"
    end

    test "renders login page with team_user_deleted flash", %{conn: conn} do
      conn = get(conn, "/auth/login", team_user_deleted: "true")
      assert html_response(conn, 200)
      assert Phoenix.Flash.get(conn.assigns.flash, :info) =~ "member profile has been deleted"
    end

    test "renders login page with invite_token param", %{conn: conn} do
      conn = get(conn, "/auth/login", invite_token: "some_token")
      assert html_response(conn, 200)
      assert Phoenix.Flash.get(conn.assigns.flash, :info) =~ "invited"
      assert get_session(conn, :invite_token) == "some_token"
    end

    test "renders login page with invite_token already in session", %{conn: conn} do
      conn =
        conn
        |> put_session(:invite_token, "existing_token")
        |> get("/auth/login")

      assert html_response(conn, 200)
      assert Phoenix.Flash.get(conn.assigns.flash, :info) =~ "invited"
    end
  end

  describe "GET /auth/login/single_tenant" do
    test "redirects to dashboard when single tenant", %{conn: conn} do
      user = insert(:user)

      stub(SingleTenant, :single_tenant?, fn -> true end)
      stub(SingleTenant, :get_default_user, fn -> user end)

      conn = get(conn, "/auth/login/single_tenant")
      assert redirected_to(conn, 302) == ~p"/dashboard"
    end

    test "redirects to stored redirect_to path when single tenant", %{conn: conn} do
      user = insert(:user)

      stub(SingleTenant, :single_tenant?, fn -> true end)
      stub(SingleTenant, :get_default_user, fn -> user end)

      conn =
        conn
        |> put_session(:redirect_to, "/some/path")
        |> get("/auth/login/single_tenant")

      assert redirected_to(conn, 302) == "/some/path"
      refute get_session(conn, :redirect_to)
    end

    test "returns 404 when not single tenant", %{conn: conn} do
      stub(SingleTenant, :single_tenant?, fn -> false end)

      conn = get(conn, "/auth/login/single_tenant")
      assert html_response(conn, 404)
    end
  end

  describe "GET /auth/email/callback/:token - new user sign up" do
    test "redirects to new source page", %{conn: conn} do
      email = "newuser_#{System.unique_integer([:positive])}@example.com"
      token = Auth.gen_email_token(email)

      conn = get(conn, "/auth/email/callback/#{token}")

      assert redirected_to(conn, 302) =~ "/sources/new"
      assert Phoenix.Flash.get(conn.assigns.flash, :info) =~ "Thanks for signing up"
    end
  end

  describe "GET /auth/email/callback/:token - existing user sign in" do
    test "redirects to dashboard", %{conn: conn} do
      user = insert(:user, provider: "email")
      token = Auth.gen_email_token(user.email)

      conn = get(conn, "/auth/email/callback/#{token}")

      assert redirected_to(conn, 302) == ~p"/dashboard"
      assert Phoenix.Flash.get(conn.assigns.flash, :info) =~ "Welcome back"
    end

    test "with oauth_params in session redirects to oauth authorization", %{conn: conn} do
      user = insert(:user, provider: "email")
      token = Auth.gen_email_token(user.email)

      oauth_params = %{
        "client_id" => "test_client",
        "redirect_uri" => "https://example.com/callback",
        "response_type" => "code",
        "scope" => "public"
      }

      conn =
        conn
        |> put_session(:oauth_params, oauth_params)
        |> get("/auth/email/callback/#{token}")

      assert redirected_to(conn, 302) =~ "client_id=test_client"
    end

    test "with vercel_setup in session redirects externally", %{conn: conn} do
      user = insert(:user, provider: "email")
      token = Auth.gen_email_token(user.email)

      vercel_setup = %{
        "auth_params" => %{
          "installation_id" => "install_#{System.unique_integer([:positive])}",
          "access_token" => "test_access_token",
          "token_type" => "Bearer",
          "vercel_user_id" => "vercel_user_#{System.unique_integer([:positive])}"
        },
        "next" => "https://vercel.com/callback"
      }

      conn =
        conn
        |> put_session(:vercel_setup, vercel_setup)
        |> get("/auth/email/callback/#{token}")

      assert redirected_to(conn, 302) == "https://vercel.com/callback"
    end
  end

  describe "GET /auth/email/callback/:token - invalid token" do
    test "redirects to login with error", %{conn: conn} do
      conn = get(conn, "/auth/email/callback/garbage_token")

      assert redirected_to(conn, 302) == ~p"/auth/login"

      assert Phoenix.Flash.get(conn.assigns.flash, :error) =~
               "There was an error signing in"
    end
  end

  describe "GET /auth/email/callback/:token - team user sign in" do
    test "team user without owner account signs in", %{conn: conn} do
      team = insert(:team)
      team_user = insert(:team_user, team: team)
      token = Auth.gen_email_token(team_user.email)

      conn = get(conn, "/auth/email/callback/#{token}")

      assert redirected_to(conn, 302) == ~p"/dashboard"
      assert Phoenix.Flash.get(conn.assigns.flash, :info) =~ "Welcome back"
    end
  end

  describe "GET /auth/email/callback/:token - with invite token in session" do
    test "with expired invite token redirects to login with error", %{conn: conn} do
      team = insert(:team)
      invitee_email = "invitee_#{System.unique_integer([:positive])}@example.com"

      email_token = Auth.gen_email_token(invitee_email)

      expired_invite_token =
        Auth.gen_email_token(team.id, signed_at: System.os_time(:second) - 90_000)

      conn =
        conn
        |> put_session(:invite_token, expired_invite_token)
        |> get("/auth/email/callback/#{email_token}")

      assert redirected_to(conn, 302) == ~p"/auth/login"
      assert Phoenix.Flash.get(conn.assigns.flash, :error) =~ "expired"
    end

    test "with invalid invite token redirects to login with error", %{conn: conn} do
      email = "test_#{System.unique_integer([:positive])}@example.com"
      email_token = Auth.gen_email_token(email)

      conn =
        conn
        |> put_session(:invite_token, "invalid_garbage_token")
        |> get("/auth/email/callback/#{email_token}")

      assert redirected_to(conn, 302) == ~p"/auth/login"
      assert Phoenix.Flash.get(conn.assigns.flash, :error) =~ "issue with this invite link"
    end

    test "with valid invite token signs in as team member", %{conn: conn} do
      owner = insert(:user)
      team = insert(:team, user: owner)
      invitee_email = "invitee_#{System.unique_integer([:positive])}@example.com"

      email_token = Auth.gen_email_token(invitee_email)
      invite_token = Auth.gen_email_token(team.id)

      conn =
        conn
        |> put_session(:invite_token, invite_token)
        |> get("/auth/email/callback/#{email_token}")

      assert redirected_to(conn, 302) == ~p"/dashboard"
      assert Phoenix.Flash.get(conn.assigns.flash, :info) =~ "Welcome to Logflare"
    end

    test "when invitee is the owner, signs in with error flash", %{conn: conn} do
      owner = insert(:user, provider: "email")
      team = insert(:team, user: owner)

      email_token = Auth.gen_email_token(owner.email)
      invite_token = Auth.gen_email_token(team.id)

      conn =
        conn
        |> put_session(:invite_token, invite_token)
        |> get("/auth/email/callback/#{email_token}")

      assert Phoenix.Flash.get(conn.assigns.flash, :error) =~ "already the owner"
    end

    test "team member limit reached redirects to login with error", %{conn: conn} do
      owner = insert(:user)
      team = insert(:team, user: owner)

      insert(:team_user, team: team)
      insert(:team_user, team: team)

      invitee_email = "overflowuser_#{System.unique_integer([:positive])}@example.com"
      email_token = Auth.gen_email_token(invitee_email)
      invite_token = Auth.gen_email_token(team.id)

      conn =
        conn
        |> put_session(:invite_token, invite_token)
        |> get("/auth/email/callback/#{email_token}")

      assert redirected_to(conn, 302) == ~p"/auth/login"
      assert Phoenix.Flash.get(conn.assigns.flash, :error) =~ "Team member limit reached"
    end
  end

  describe "POST /account - create_and_sign_in" do
    test "creates a user from team_user and signs in", %{conn: conn} do
      team_user = insert(:team_user)

      conn =
        conn
        |> put_session(:current_email, team_user.email)
        |> post("/account")

      assert redirected_to(conn, 302) == ~p"/dashboard"
    end
  end
end
