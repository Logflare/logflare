defmodule LogflareWeb.Plugs.RateLimiterTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  alias LogflareWeb.Plugs.RateLimiter

  @verify_true {:ok,
                %{
                  message: "",
                  metrics: %{
                    user: %{
                      remaining: 50,
                      limit: 100
                    },
                    source: %{
                      remaining: 50,
                      limit: 100
                    }
                  }
                }}

  @verify_false {:error,
                 %{
                   message: "ERR",
                   metrics: %{
                     user: %{
                       remaining: -1,
                       limit: 100
                     },
                     source: %{
                       remaining: 20,
                       limit: 100
                     }
                   }
                 }}

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
      expect(Logflare.Users.APIMock, :verify_api_rates_quotas, fn _ -> @verify_true end)

      conn =
        build_conn(:get, "/api")
        |> Plug.Conn.assign(:user, %{id: 1})
        |> RateLimiter.call(@params)

      assert {"x-rate-limit-user_limit", "100"} in conn.resp_headers
      assert {"x-rate-limit-user_remaining", "50"} in conn.resp_headers
      assert {"x-rate-limit-source_limit", "100"} in conn.resp_headers
      assert {"x-rate-limit-source_remaining", "50"} in conn.resp_headers
      assert conn.status == nil
      assert conn.halted == false
    end

    test "halts when POST logs action is not allowed" do
      expect(Logflare.Users.APIMock, :verify_api_rates_quotas, fn _ -> @verify_false end)

      conn =
        build_conn(:get, "/api")
        |> Plug.Conn.assign(:user, %{id: 1})
        |> RateLimiter.call(@params)

      assert {"x-rate-limit-user_limit", "100"} in conn.resp_headers
      assert {"x-rate-limit-user_remaining", "-1"} in conn.resp_headers
      assert {"x-rate-limit-source_limit", "100"} in conn.resp_headers
      assert {"x-rate-limit-source_remaining", "20"} in conn.resp_headers
      assert conn.resp_body == "ERR"
      assert conn.status == 429
      assert conn.halted == true
    end
  end
end
