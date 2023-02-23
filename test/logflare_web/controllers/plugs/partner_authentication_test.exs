defmodule LogflareWeb.Plugs.PartnerAuthenticationTest do
  use LogflareWeb.ConnCase
  alias LogflareWeb.Plugs.PartnerAuthentication

  test "halts connection with 403 code if partner identifying token isn't present in params" do
    conn = build_conn(:get, "/")

    conn = PartnerAuthentication.call(conn)
    assert json_response(conn, 401)
    assert conn.halted()
  end

  test "halts connection with 403 code if user does not have the API Bearer token set in the request" do
    conn = build_conn(:get, "/", %{token: TestUtils.gen_uuid()})

    conn = PartnerAuthentication.call(conn)

    assert json_response(conn, 401)
    assert conn.halted
  end

  test "halts connection with 403 code if user does has the wrong API Bearer token set in the request" do
    conn = build_conn(:get, "/", %{token: TestUtils.gen_uuid()})

    conn =
      conn
      |> put_req_header("authorization", "Bearer potato")
      |> PartnerAuthentication.call()

    assert json_response(conn, 401)
    assert conn.halted
  end

  test "proceeds connection and assigns partner if the user has the right the API Bearer token set in the request" do
    %{token: token, auth_token: auth_token} = partner = insert(:partner)
    conn = build_conn(:get, "/", %{token: token})

    conn =
      conn
      |> put_req_header("authorization", "Bearer #{auth_token}")
      |> PartnerAuthentication.call()

    refute conn.halted
    assert conn.assigns.partner == partner
  end
end
