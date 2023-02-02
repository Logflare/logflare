defmodule LogflareWeb.Plugs.EnsureSuperUserAuthenticationTest do
  use LogflareWeb.ConnCase
  alias LogflareWeb.Plugs.EnsureSuperUserAuthentication

  test "halts connection with 403 code if user does not have the API Bearer token set in the request",
       %{
         conn: conn
       } do
    conn = EnsureSuperUserAuthentication.call(conn)
    assert json_response(conn, 401)
    assert conn.halted()
  end

  test "halts connection with 403 code if user does has the wrong API Bearer token set in the request",
       %{
         conn: conn
       } do
    conn =
      conn
      |> put_req_header("authorization", "Bearer potato")
      |> EnsureSuperUserAuthentication.call()

    assert json_response(conn, 401)
    assert conn.halted()
  end

  test "proceeds connection if the user has the right the API Bearer token set in the request", %{
    conn: conn
  } do
    [token: token] =
      Application.get_env(:logflare, LogflareWeb.Plugs.EnsureSuperUserAuthentication)

    conn =
      conn
      |> put_req_header("authorization", "Bearer #{token}")
      |> EnsureSuperUserAuthentication.call()

    refute conn.halted()
  end
end
