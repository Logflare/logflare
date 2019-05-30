defmodule LogflareWeb.Plugs.SetApiUserTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  alias LogflareWeb.Plugs.SetApiUser
  @api_key "1337"
  @mock_module Logflare.AccountCacheMock
  import Logflare.DummyFactory

  import Mox

  setup :verify_on_exit!

  setup do
    :ok = Ecto.Adapters.SQL.Sandbox.mode(Logflare.Repo, {:shared, self()})
  end

  setup do
    u1 = insert(:user, %{api_key: @api_key})
    u2 = insert(:user, %{api_key: nil})
    {:ok, users: [u1, u2]}
  end

  describe "Plugs.SetApiUser" do
    test "sets api user correctly", %{users: [u1 | _]} do
      conn =
        build_conn(:post, "/")
        |> put_req_header("x-api-key", @api_key)
        |> SetApiUser.call(%{})

      assert conn.assigns.user.id == u1.id
      assert conn.halted == false
    end

    test "doesn't set user if api_key is not verified" do
      conn =
        build_conn(:post, "/")
        |> put_req_header("x-api-key", "")
        |> SetApiUser.call(%{})

      assert conn.assigns.user == nil
      assert conn.halted == false
    end
  end
end
