defmodule LogflareWeb.Plugs.RateLimiterTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  alias LogflareWeb.Plugs.RateLimiter

  describe "rate limiter plug works correctly" do
    test "doesn't halt when POST logs action is allowed" do
      plan = insert(:plan, limit_rate_limit: 5)
      u = insert(:user)
      s = insert(:source, user_id: u.id)

      conn =
        build_conn(:get, "/api")
        |> assign(:user, u)
        |> assign(:source, s)
        |> assign(:plan, plan)
        |> RateLimiter.call()

      assert {"x-rate-limit-user_limit", "300"} in conn.resp_headers
      assert {"x-rate-limit-user_remaining", "300"} in conn.resp_headers
      assert {"x-rate-limit-source_limit", "3000"} in conn.resp_headers
      assert {"x-rate-limit-source_remaining", "3000"} in conn.resp_headers
      assert conn.status == nil
      assert conn.halted == false
    end

    test "halts when POST logs action is not allowed" do
      plan = insert(:plan, limit_rate_limit: 0)
      u = insert(:user)
      s = insert(:source, user_id: u.id)

      conn =
        build_conn(:get, "/api")
        |> Plug.Conn.assign(:user, u)
        |> Plug.Conn.assign(:source, s)
        |> Plug.Conn.assign(:plan, plan)
        |> RateLimiter.call()

      assert {"x-rate-limit-user_limit", "0"} in conn.resp_headers
      assert {"x-rate-limit-user_remaining", "0"} in conn.resp_headers
      assert {"x-rate-limit-source_limit", "3000"} in conn.resp_headers
      assert {"x-rate-limit-source_remaining", "3000"} in conn.resp_headers

      assert json_response(conn, 429) == %{
               "error" =>
                 "User rate is over the API quota. Email support@logflare.app to increase your rate limit."
             }
    end
  end
end
