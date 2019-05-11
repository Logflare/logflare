defmodule LogflareWeb.Plugs.SetApiUserTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  alias LogflareWeb.Plugs.SetApiUser
  @api_key "1337"
  @mock_module Logflare.AccountCacheMock

  import Mox

  setup :verify_on_exit!

  setup_all do
    Mox.defmock(Logflare.AccountCacheMock, for: Logflare.AccountCache)
    :ok
  end

  describe "Plugs.SetApiUser" do
    test "sets api user correctly" do
      verify_fn = fn api_key -> @api_key == api_key end
      expect(Logflare.AccountCacheMock, :verify_account?, verify_fn)

      conn =
        build_conn(:post, "/")
        |> put_req_header("x-api-key", @api_key)
        |> SetApiUser.call(%{})

      assert conn.assigns.user == @api_key
      assert conn.halted == false
    end

    test "doesn't set user if api_key is not verified" do
      verify_fn = fn api_key -> @api_key == api_key end
      expect(Logflare.AccountCacheMock, :verify_account?, verify_fn)

      conn =
        build_conn(:post, "/")
        |> put_req_header("x-api-key", "1111")
        |> SetApiUser.call(%{})

      assert conn.assigns.user == nil
      assert conn.halted == false
    end
  end
end
