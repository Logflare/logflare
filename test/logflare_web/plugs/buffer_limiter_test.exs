defmodule LogflareWeb.Plugs.BufferLimiterTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  alias LogflareWeb.Plugs.BufferLimiter
  alias Logflare.PubSubRates

  describe "partner impersonation" do
    setup do
      conn = build_conn(:post, "/api/logs", %{"message" => "some text"})
      {:ok, conn: conn}
    end

    test "if buffer is full, return 429", %{conn: conn} do
      source = insert(:source, user: insert(:user))
      PubSubRates.Cache.cache_buffers(source.token, nil, %{Node.self() => 20_000})

      conn =
        conn
        |> assign(:source, source)
        |> BufferLimiter.call(%{})

      assert conn.halted == true
      assert conn.status == 429
    end

    test "if buffer not full, passthrough", %{conn: conn} do
      source = insert(:source, user: insert(:user))
      PubSubRates.Cache.cache_buffers(source.token, nil, %{"localhost" => 1})

      conn =
        conn
        |> assign(:source, source)
        |> BufferLimiter.call(%{})

      assert conn.halted == false
    end
  end
end
