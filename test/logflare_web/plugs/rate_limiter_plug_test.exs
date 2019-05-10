defmodule LogflareWeb.Plugs.RateLimiterTest do
  use LogflareWeb.ConnCase
  alias LogflareWeb.Plugs.RateLimiter

  import Mox

  @existing_source_id :dummy_source_id
  @params %{"source" => Atom.to_string(@existing_atom)}

  setup :verify_on_exit!

  setup_all do
    Mox.defmock(Logflare.Users.APIMock, for: Logflare.Users.API)
    :ok 
  end

  describe "rate limiter plug works correctly" do
    test "doesn't halt when POST logs action is allowed" do
      expect(Logflare.Users.APIMock, :action_allowed?, fn _ -> true end)

      conn =
        build_conn(:get, "/api")
        |> Plug.Conn.assign(:user, %{id: 1})
        |> RateLimiter.call(@params)

      assert conn.status == nil
      assert conn.halted == false
    end

    test "halts when POST logs action is not allowed" do
      expect(Logflare.Users.APIMock, :action_allowed?, fn _ -> false end)

      conn =
        build_conn(:get, "/api")
        |> Plug.Conn.assign(:user, %{id: 1})
        |> RateLimiter.call(@params)

      assert conn.status == 429
      assert conn.halted == true
    end
  end
end
