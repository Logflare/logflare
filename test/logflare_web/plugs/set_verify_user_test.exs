defmodule LogflareWeb.Plugs.SetVerifyUserTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  alias LogflareWeb.Plugs.SetVerifyUser
  import Logflare.Factory

  setup do
    u1 = insert(:user)
    u2 = insert(:user)
    {:ok, users: [u1, u2]}
  end

  describe "Plugs.SetVerifyUser" do
    test "sets api user correctly", %{users: [u1 | _]} do
      conn =
        build_conn(:post, "/logs")
        |> put_req_header("x-api-key", u1.api_key)
        |> SetVerifyUser.call(%{})

      assert conn.assigns.user.id == u1.id
      assert conn.halted == false
    end

    test "halts api request with no api key" do
      conn =
        build_conn(:post, "/logs")
        |> put_req_header("x-api-key", "")
        |> SetVerifyUser.call(%{})

      assert conn.halted == true
      assert conn.assigns.message == "Error: please set API token"
    end
  end
end
