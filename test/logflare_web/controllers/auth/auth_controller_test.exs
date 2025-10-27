defmodule LogflareWeb.AuthControllerTest do
  use LogflareWeb.ConnCase, async: false

  setup do
    insert(:plan)
    :ok
  end

  test "logout clears session and redirects to login", %{conn: conn} do
    user = insert(:user)

    conn =
      conn
      |> login_user(user)
      |> put_session(:some_data, "test")
      |> get("/auth/logout")

    assert redirected_to(conn, 302) == ~p"/auth/login"
    assert conn.resp_cookies["_logflare_user_id"].value == ""
    assert conn.resp_cookies["_logflare_team_user_id"].value == ""
    assert conn.resp_cookies["_logflare_last_team"].value == ""
    assert conn.resp_cookies["_logflare_last_provider"].value == ""
  end
end
