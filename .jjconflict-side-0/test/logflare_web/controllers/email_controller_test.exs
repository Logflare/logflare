defmodule LogflareWeb.EmailControllerTest do
  use LogflareWeb.ConnCase

  test "GET email login", %{conn: conn} do
    conn =
      conn
      |> get(~p"/auth/login/email")

    assert html_response(conn, 200) =~ "email"
    assert conn.assigns.flash["error"] == nil
  end

  test "POST email login", %{conn: conn} do
    conn =
      conn
      |> post(~p"/auth/login/email", %{email: "some-email@logflare.app"})

    assert html_response(conn, 302)
    assert conn.assigns.flash["error"] == nil
  end

  test "POST email login with empty email", %{conn: conn} do
    conn =
      conn
      |> post(~p"/auth/login/email", %{email: ""})

    assert html_response(conn, 200)
    assert conn.assigns.flash["error"] =~ "cannot be empty"
  end
end
