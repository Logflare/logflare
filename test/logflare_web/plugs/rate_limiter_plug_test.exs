defmodule LogflareWeb.Plugs.RateLimiterTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  alias Logflare.{Users, Sources}
  alias LogflareWeb.Plugs.RateLimiter
  alias Logflare.Sources.Source.RateCounterServer

  @moduletag :skip

  setup do
    u1 = insert(:user, api_key: "dummy_key", api_quota: 5)
    u2 = insert(:user, api_key: "other_dummy_key", api_quota: 0)
    s1 = insert(:source, user_id: u1.id)
    s2 = insert(:source, user_id: u2.id)

    u1 = Users.preload_defaults(u1)
    u2 = Users.preload_defaults(u2)

    s1 = Sources.get_by(id: s1.id)
    s2 = Sources.get_by(id: s2.id)

    {:ok, _} = RateCounterServer.start_link(%{source_id: s1.token})
    {:ok, _} = RateCounterServer.start_link(%{source_id: s2.token})
    Process.sleep(5_000)

    {:ok, users: [u1, u2], sources: [s1, s2]}
  end

  describe "rate limiter plug works correctly" do
    test "doesn't halt when POST logs action is allowed", %{users: [u | _], sources: [s, _]} do
      conn =
        build_conn(:get, "/api")
        |> assign(:user, u)
        |> assign(:source, s)
        |> RateLimiter.call()

      assert {"x-rate-limit-user_limit", "300"} in conn.resp_headers
      assert {"x-rate-limit-user_remaining", "300"} in conn.resp_headers
      assert {"x-rate-limit-source_limit", "3000"} in conn.resp_headers
      assert {"x-rate-limit-source_remaining", "3000"} in conn.resp_headers
      assert conn.status == nil
      assert conn.halted == false
    end

    test "halts when POST logs action is not allowed", %{users: [_, u], sources: [_, s]} do
      conn =
        build_conn(:get, "/api")
        |> Plug.Conn.assign(:user, u)
        |> Plug.Conn.assign(:source, s)
        |> RateLimiter.call()

      assert {"x-rate-limit-user_limit", "0"} in conn.resp_headers
      assert {"x-rate-limit-user_remaining", "0"} in conn.resp_headers
      assert {"x-rate-limit-source_limit", "3000"} in conn.resp_headers
      assert {"x-rate-limit-source_remaining", "3000"} in conn.resp_headers

      assert conn.resp_body ==
               "User rate is over the API quota. Email support@logflare.app to increase your rate limit."

      assert conn.status == 429
      assert conn.halted == true
    end
  end
end
