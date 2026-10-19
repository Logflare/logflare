defmodule LogflareWeb.Plugs.RateLimiterTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  alias LogflareWeb.Plugs.RateLimiter

  describe "multi-source mode" do
    test "passes when all declared sources are within rate limits" do
      plan = insert(:plan, limit_rate_limit: 5)
      user = insert(:user)
      source_a = insert(:source, user_id: user.id)
      source_b = insert(:source, user_id: user.id)

      conn =
        build_conn(:post, "/logs")
        |> assign(:user, user)
        |> assign(:source, nil)
        |> assign(:plan, plan)
        |> assign(:declared_sources, %{
          Atom.to_string(source_a.token) => source_a,
          Atom.to_string(source_b.token) => source_b
        })
        |> RateLimiter.call()

      refute conn.halted
    end

    test "halts with 429 when any declared source exceeds its rate limit" do
      plan = insert(:plan, limit_source_rate_limit: 0)
      user = insert(:user)
      source_a = insert(:source, user_id: user.id)
      source_b = insert(:source, user_id: user.id)

      conn =
        build_conn(:post, "/logs")
        |> assign(:user, user)
        |> assign(:source, nil)
        |> assign(:plan, plan)
        |> assign(:declared_sources, %{
          Atom.to_string(source_a.token) => source_a,
          Atom.to_string(source_b.token) => source_b
        })
        |> RateLimiter.call()

      assert conn.halted

      assert json_response(conn, 429) == %{
               "error" =>
                 "Source rate is over the API quota. Email support@logflare.app to increase your rate limit."
             }
    end

    test "checks the query-param source alongside declared sources, deduplicated by id" do
      plan = insert(:plan, limit_rate_limit: 5)
      user = insert(:user)
      default_source = insert(:source, user_id: user.id)
      source_b = insert(:source, user_id: user.id)
      this = self()

      Logflare.Users.API
      |> stub(:verify_api_rates_quotas, fn %{source: source} ->
        send(this, {:checked, source.id})
        {:ok, %{metrics: %{}}}
      end)

      conn =
        build_conn(:post, "/logs")
        |> assign(:user, user)
        |> assign(:source, default_source)
        |> assign(:plan, plan)
        |> assign(:declared_sources, %{
          Atom.to_string(default_source.token) => %{default_source | metrics: nil},
          Atom.to_string(source_b.token) => source_b
        })
        |> RateLimiter.call()

      refute conn.halted

      checked =
        for _ <- 1..2 do
          assert_received {:checked, id}
          id
        end

      assert Enum.sort(checked) == Enum.sort([default_source.id, source_b.id])
      refute_received {:checked, _}
    end

    test "halts with 429 when the query-param source exceeds its rate limit" do
      plan = insert(:plan, limit_rate_limit: 5)
      user = insert(:user)
      default_source = insert(:source, user_id: user.id)
      source_b = insert(:source, user_id: user.id)
      default_source_id = default_source.id

      metrics = %{user: %{limit: 1, remaining: 0}, source: %{limit: 1, remaining: 0}}

      Logflare.Users.API
      |> stub(:verify_api_rates_quotas, fn
        %{source: %{id: ^default_source_id}} -> {:error, %{message: "over", metrics: metrics}}
        _ -> {:ok, %{metrics: metrics}}
      end)

      conn =
        build_conn(:post, "/logs")
        |> assign(:user, user)
        |> assign(:source, default_source)
        |> assign(:plan, plan)
        |> assign(:declared_sources, %{Atom.to_string(source_b.token) => source_b})
        |> RateLimiter.call()

      assert conn.halted
      assert json_response(conn, 429) == %{"error" => "over"}
    end
  end

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
