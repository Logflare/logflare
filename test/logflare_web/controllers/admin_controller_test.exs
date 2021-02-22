defmodule LogflareWeb.AdminControllerTest do
  @moduledoc false
  import Logflare.Factory
  use LogflareWeb.ConnCase

  describe "Admin controller" do
    setup do
      s1u1 = build(:source, rules: [])
      u1 = Users.insert_or_update_user(params_for(:user, sources: [s1u1]))
      s1u2 = build(:source, rules: [])
      u2 = Users.insert_or_update_user(params_for(:user, sources: [s1u2]))

      a1 = Users.insert_or_update_user(params_for(:user, admin: true))

      sources = [s1u1, s1u2]
      users = [u1, u2]

      {:ok, users: users, sources: sources, admins: [a1]}
    end

    test "halts and returns 401 for non-admin user", %{
      conn: conn,
      users: [u1 | _],
      sources: _sources
    } do
      conn =
        conn
        |> assign(:user, u1)
        |> get("/admin/dashboard")

      assert conn.halted == true
      assert conn.status == 403
    end

    test "renders dashboard for admin", %{
      conn: conn,
      admins: [a1],
      sources: _sources
    } do
      conn =
        conn
        |> assign(:user, a1)
        |> get("/admin/dashboard")

      assert conn.halted == false
      assert html_response(conn, 200) =~ "~/admin"
      assert html_response(conn, 200) =~ "source"
    end
  end
end
