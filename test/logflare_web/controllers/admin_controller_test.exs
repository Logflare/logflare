defmodule LogflareWeb.AdminControllerTest do
  @moduledoc false
  import Logflare.DummyFactory
  use LogflareWeb.ConnCase

  describe "Admin controller" do
    setup do
      s1u1 = insert(:source, rules: [])
      u1 = insert(:user, sources: [s1u1])
      s1u2 = insert(:source, rules: [])
      u2 = insert(:user, sources: [s1u2])

      a1 = insert(:user, admin: true)

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
