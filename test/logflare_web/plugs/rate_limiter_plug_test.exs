defmodule LogflareWeb.Plugs.RateLimiterTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  alias LogflareWeb.Plugs.RateLimiter
  import Logflare.DummyFactory

  setup do
    s1 = insert(:source, token: Faker.UUID.v4())
    s2 = insert(:source, token: Faker.UUID.v4())
    u1 = insert(:user, sources: [s1], api_key: "dummy_key", api_quota: 5)
    u2 = insert(:user, sources: [s2], api_key: "other_dummy_key", api_quota: 0)
    {:ok, _} = Logflare.SourceRateCounter.start_link(s1.token)
    {:ok, _} = Logflare.SourceRateCounter.start_link(s2.token)
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
