defmodule LogflareWeb.Plugs.BufferLimiterTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  alias LogflareWeb.Plugs.BufferLimiter
  alias Logflare.PubSubRates
  alias Logflare.Backends

  describe "partner impersonation" do
    setup do
      conn = build_conn(:post, "/api/logs", %{"message" => "some text"})

      on_exit(fn ->
        PubSubRates.Cache
        |> Cachex.clear()
      end)

      {:ok, conn: conn}
    end

    test "if buffer is full, return 429", %{conn: conn} do
      source = insert(:source, user: insert(:user))
      Backends.set_buffer_len(source, nil, 20_000)

      conn =
        conn
        |> assign(:source, source)
        |> BufferLimiter.call(%{})

      assert conn.halted == true
      assert conn.status == 429
    end

    test "check if buffer is full if no node value is found", %{conn: conn} do
      source = insert(:source, user: insert(:user))

      conn =
        conn
        |> assign(:source, source)
        |> BufferLimiter.call(%{})

      assert conn.halted == false
    end

    test "if buffer not full, passthrough", %{conn: conn} do
      source = insert(:source, user: insert(:user))
      Backends.set_buffer_len(source, nil, 1)

      conn =
        conn
        |> assign(:source, source)
        |> BufferLimiter.call(%{})

      assert conn.halted == false
    end
  end
end
