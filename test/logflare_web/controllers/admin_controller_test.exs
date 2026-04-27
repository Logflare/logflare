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

  describe "shutdown_node" do
    @shutdown_code "test-shutdown-code"

    setup do
      prev = Application.get_env(:logflare, :node_shutdown_code)
      on_exit(fn -> Application.put_env(:logflare, :node_shutdown_code, prev) end)
      :ok
    end

    test "returns 401 when server code is nil and no header sent", %{conn: conn} do
      Application.put_env(:logflare, :node_shutdown_code, nil)
      conn = put(conn, "/admin/shutdown")
      assert json_response(conn, 401)
    end

    test "returns 401 when server code is nil even if header is sent (bypass regression)", %{
      conn: conn
    } do
      Application.put_env(:logflare, :node_shutdown_code, nil)
      conn = conn |> put_req_header("x-shutdown-code", "anything") |> put("/admin/shutdown")
      assert json_response(conn, 401)
    end

    test "returns 401 when server code is set but header is missing", %{conn: conn} do
      Application.put_env(:logflare, :node_shutdown_code, @shutdown_code)
      conn = put(conn, "/admin/shutdown")
      assert json_response(conn, 401)
    end

    test "returns 401 when server code is set but header is wrong", %{conn: conn} do
      Application.put_env(:logflare, :node_shutdown_code, @shutdown_code)
      conn = conn |> put_req_header("x-shutdown-code", "wrong") |> put("/admin/shutdown")
      assert json_response(conn, 401)
    end

    test "returns 401 when secret is sent as query param (old channel)", %{conn: conn} do
      Application.put_env(:logflare, :node_shutdown_code, @shutdown_code)
      conn = put(conn, "/admin/shutdown?code=#{@shutdown_code}")
      assert json_response(conn, 401)
    end

    test "returns 200 and shuts down self when header matches and no node param", %{conn: conn} do
      Application.put_env(:logflare, :node_shutdown_code, @shutdown_code)
      expect(Logflare.Admin, :shutdown, fn -> :ok end)

      conn =
        conn
        |> put_req_header("x-shutdown-code", @shutdown_code)
        |> put("/admin/shutdown")

      assert %{"message" => _} = json_response(conn, 200)
    end

    test "returns 200 and shuts down specified node when node param matches", %{conn: conn} do
      Application.put_env(:logflare, :node_shutdown_code, @shutdown_code)
      node_str = Atom.to_string(Node.self())
      expect(Logflare.Admin, :shutdown, fn _node -> :ok end)

      conn =
        conn
        |> put_req_header("x-shutdown-code", @shutdown_code)
        |> put("/admin/shutdown", %{"node" => node_str})

      assert %{"message" => _} = json_response(conn, 200)
    end
  end
end
