defmodule LogflareWeb.Plugs.BufferLimiterTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  alias LogflareWeb.Plugs.BufferLimiter
  alias Logflare.Backends.IngestEventQueue

  setup do
    conn = build_conn(:post, "/api/logs", %{"message" => "some text"})
    source = insert(:source, user: insert(:user))
    IngestEventQueue.upsert_tid({source, nil})
    {:ok, conn: conn, source: source}
  end

  test "if buffer is full of pending, return 429", %{conn: conn, source: source} do
    for _ <- 1..55_000 do
      le = build(:log_event)
      IngestEventQueue.add_to_table({source, nil}, [le])
    end

    conn =
      conn
      |> assign(:source, source)
      |> BufferLimiter.call(%{})

    assert conn.halted == true
    assert conn.status == 429
  end

  test "200 if most events are ingested", %{conn: conn, source: source} do
    for _ <- 1..8_000 do
      le = build(:log_event)
      IngestEventQueue.add_to_table({source, nil}, [le])
      IngestEventQueue.mark_ingested({source, nil}, [le])
    end

    conn =
      conn
      |> assign(:source, source)
      |> BufferLimiter.call(%{})

    assert conn.halted == false
  end

  test "200 for uninitialized table", %{conn: conn} do
    source = insert(:source, user: insert(:user))

    conn =
      conn
      |> assign(:source, source)
      |> BufferLimiter.call(%{})

    assert conn.halted == false
  end

  test "if buffer not full, passthrough", %{conn: conn} do
    source = insert(:source, user: insert(:user))

    conn =
      conn
      |> assign(:source, source)
      |> BufferLimiter.call(%{})

    assert conn.halted == false
  end
end
