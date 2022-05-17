defmodule LogflareWeb.Plugs.VerifyApiAccessTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  alias LogflareWeb.Plugs.VerifyApiAccess
  import Logflare.Factory

  setup do
    user = insert(:user)
    endpoint = insert(:endpoint, user: user)
    {:ok, token} = Logflare.Auth.create_access_token(user)
    conn = build_conn(:post, "/endpoints/query/:token", %{"token" => endpoint.token})

    :meck.expect(Logflare.SQL, :source_mapping, fn _, _, _ -> {:ok, "the query"} end)

    on_exit(fn ->
      :meck.reset(Logflare.SQL)
    end)

    {:ok, conn: conn, user: user, token: token, endpoint: endpoint}
  end

  describe "By x-api-key header" do
    test "verifies correctly", %{conn: conn, user: user, token: token} do
      conn =
        conn
        |> put_req_header("x-api-key", token.token)
        |> VerifyApiAccess.call(%{})

      assert conn.halted == false
      assert conn.assigns.user.id == user.id
    end

    test "halts request with no api key", %{conn: conn} do
      conn =
        conn
        |> put_req_header("x-api-key", "123")
        |> VerifyApiAccess.call(%{})

      assert_unauthorized(conn)
    end

    test "halts endpoint request with token from different user", %{conn: conn} do
      user2 = insert(:user)
      {:ok, token2} = Logflare.Auth.create_access_token(user2)

      conn =
        conn
        |> put_req_header("x-api-key", token2.token)
        |> VerifyApiAccess.call(%{})

      assert_unauthorized(conn)
    end
  end

  defp assert_unauthorized(conn) do
    assert conn.halted == true
    assert conn.assigns.message |> String.downcase() =~ "error"
    assert conn.assigns.message |> String.downcase() =~ "unauthorized"
    assert conn.status == 401
  end
end
