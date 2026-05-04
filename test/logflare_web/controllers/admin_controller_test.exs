defmodule LogflareWeb.AdminControllerTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  describe "Admin controller" do
    setup do
      insert(:plan)
      {:ok, admin: insert(:user, admin: true), user: insert(:user)}
    end

    test "halts and returns 401 for non-admin user", %{conn: conn, user: user} do
      conn =
        conn
        |> login_user(user)
        |> get("/admin/dashboard")

      assert conn.halted == true
      assert conn.status == 403
    end

    test "renders dashboard for admin", %{
      conn: conn,
      admin: admin
    } do
      conn =
        conn
        |> login_user(admin)
        |> get("/admin/dashboard")

      assert conn.halted == false
      assert html_response(conn, 200) =~ "~/admin"
    end

    test "become functionality lets an admin turn into a google user", %{conn: conn, admin: admin} do
      user = insert(:user, provider_uid: "google")

      conn =
        conn
        |> login_user(admin)
        |> get(~p"/admin/accounts/#{user.id}/become")

      assert redir_path = redirected_to(conn)
      assert ~p"/dashboard" == redir_path
      conn = get(recycle(conn), redir_path)
      assert html = html_response(conn, 200)
      assert html =~ user.email
      assert html =~ user.name
    end

    test "bug: become account when cookies are set due to being team_user of a team", %{
      conn: conn,
      admin: admin
    } do
      user = insert(:user, provider_uid: "google-123")
      user_team = insert(:team, user: user)

      # admin has home team and is  team user
      admin_team = insert(:team, user: admin)

      attrs =
        Map.take(admin, [
          :email,
          :email_preferred,
          :name,
          :phone,
          :image,
          :provider,
          :provider_uid,
          :token
        ])
        |> Map.to_list()

      insert(:team_user, attrs ++ [team: admin_team, valid_google_account: true])

      conn =
        conn
        |> login_user(admin)
        |> put_resp_cookie("_logflare_user_id", inspect(admin.id), max_age: 2_592_000)
        |> put_resp_cookie("_logflare_team_user_id", inspect(user_team.id), max_age: 2_592_000)
        |> get(~p"/admin/accounts/#{user.id}/become")

      assert redir_path = redirected_to(conn)
      assert ~p"/dashboard" == redir_path
      conn = get(recycle(conn), redir_path)
      assert html = html_response(conn, 200)
      assert html =~ user.email
      assert html =~ user.name
      assert html =~ user_team.name
      refute html =~ admin.name
      refute html =~ admin_team.name
    end

    test "admin can grant admin to another user", %{conn: conn, admin: admin} do
      target = insert(:user, admin: false)

      conn =
        conn
        |> login_user(admin)
        |> post(~p"/admin/accounts/#{target.id}/grant_admin")

      assert redirected_to(conn) == ~p"/admin/accounts"
      assert Phoenix.Flash.get(conn.assigns.flash, :info) == "Admin access granted!"
      assert Logflare.Users.get(target.id).admin == true
    end

    test "non-admin cannot grant admin (403)", %{conn: conn, user: user} do
      target = insert(:user, admin: false)

      conn =
        conn
        |> login_user(user)
        |> post(~p"/admin/accounts/#{target.id}/grant_admin")

      assert conn.halted == true
      assert conn.status == 403
      refute Logflare.Users.get(target.id).admin
    end

    test "become_account redirects with error flash for a non-existent user ID", %{
      conn: conn,
      admin: admin
    } do
      conn =
        conn
        |> login_user(admin)
        |> get(~p"/admin/accounts/0/become")

      assert redirected_to(conn) == ~p"/admin/accounts"
      assert Phoenix.Flash.get(conn.assigns.flash, :error) == "Account not found."
    end

    test "grant_admin redirects with error flash for a non-existent target user ID", %{
      conn: conn,
      admin: admin
    } do
      conn =
        conn
        |> login_user(admin)
        |> post(~p"/admin/accounts/0/grant_admin")

      assert redirected_to(conn) == ~p"/admin/accounts"
      assert Phoenix.Flash.get(conn.assigns.flash, :error) == "Account not found."
    end

    test "revoke_admin redirects with error flash for a non-existent target user ID", %{
      conn: conn,
      admin: admin
    } do
      conn =
        conn
        |> login_user(admin)
        |> post(~p"/admin/accounts/0/revoke_admin")

      assert redirected_to(conn) == ~p"/admin/accounts"
      assert Phoenix.Flash.get(conn.assigns.flash, :error) == "Account not found."
    end

    test "admin can revoke admin from another admin", %{conn: conn, admin: admin} do
      target = insert(:user, admin: true)

      conn =
        conn
        |> login_user(admin)
        |> post(~p"/admin/accounts/#{target.id}/revoke_admin")

      assert redirected_to(conn) == ~p"/admin/accounts"
      assert Phoenix.Flash.get(conn.assigns.flash, :info) == "Admin access revoked."
      refute Logflare.Users.get(target.id).admin
    end

    test "revoke_admin is blocked when target is the last admin", %{conn: conn, admin: admin} do
      conn =
        conn
        |> login_user(admin)
        |> post(~p"/admin/accounts/#{admin.id}/revoke_admin")

      assert redirected_to(conn) == ~p"/admin/accounts"
      assert Phoenix.Flash.get(conn.assigns.flash, :error) =~ "last admin"
      assert Logflare.Users.get(admin.id).admin
    end

    test "admin cannot revoke their own admin access", %{conn: conn, admin: admin} do
      conn =
        conn
        |> login_user(admin)
        |> post(~p"/admin/accounts/#{admin.id}/revoke_admin")

      assert redirected_to(conn) == ~p"/admin/accounts"

      assert Phoenix.Flash.get(conn.assigns.flash, :error) ==
               "Cannot revoke your own admin access."

      assert Logflare.Users.get(admin.id).admin
    end

    test "non-admin cannot revoke admin (403)", %{conn: conn, user: user} do
      target = insert(:user, admin: true)

      conn =
        conn
        |> login_user(user)
        |> post(~p"/admin/accounts/#{target.id}/revoke_admin")

      assert conn.halted == true
      assert conn.status == 403
      assert Logflare.Users.get(target.id).admin
    end

    test "admin can delete an account", %{conn: conn, admin: admin} do
      target = insert(:user)

      conn =
        conn
        |> login_user(admin)
        |> delete(~p"/admin/accounts/#{target.id}")

      assert redirected_to(conn) == ~p"/admin/accounts"
      assert Phoenix.Flash.get(conn.assigns.flash, :info) == "Account deleted!"
      refute Logflare.Users.get(target.id)
    end

    test "non-admin user cannot delete an account (403)", %{conn: conn, user: user} do
      target = insert(:user)

      conn =
        conn
        |> login_user(user)
        |> delete(~p"/admin/accounts/#{target.id}")

      assert conn.halted == true
      assert conn.status == 403
      assert Logflare.Users.get(target.id)
    end

    test "bug: become account clears last_switched_team_id from session", %{
      conn: conn,
      admin: admin
    } do
      user = insert(:user, provider_uid: "google-456")
      user_team = insert(:team, user: user)

      # admin has their own team
      admin_team = insert(:team, user: admin)

      conn =
        conn
        |> login_user(admin)
        |> Plug.Test.init_test_session(%{last_switched_team_id: admin_team.id})
        |> get(~p"/admin/accounts/#{user.id}/become")

      assert redir_path = redirected_to(conn)
      assert ~p"/dashboard" == redir_path
      refute get_session(conn, :last_switched_team_id)
      conn = get(recycle(conn), redir_path)
      assert html = html_response(conn, 200)
      assert html =~ user.email
      assert html =~ user.name
      assert html =~ user_team.name
      refute html =~ admin_team.name
    end
  end
end
