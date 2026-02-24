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
