defmodule LogflareWeb.Plugs.BufferLimiterTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  alias LogflareWeb.Plugs.BufferLimiter
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends

  setup do
    insert(:plan)
    conn = build_conn(:post, "/api/logs", %{"message" => "some text"})
    source = insert(:source, user: insert(:user))
    table_key = {source.id, nil, self()}
    IngestEventQueue.upsert_tid(table_key)
    {:ok, conn: conn, source: source, table_key: table_key}
  end

  test "if buffer is full of pending, return 429", %{
    conn: conn,
    source: source,
    table_key: table_key
  } do
    for _ <- 1..100_500 do
      le = build(:log_event)
      IngestEventQueue.add_to_table(table_key, [le])
    end

    # get and cache the value
    Backends.cache_local_buffer_lens(source.id, nil)

    conn =
      conn
      |> assign(:source, source)
      |> BufferLimiter.call(%{})

    assert conn.halted == true
    assert conn.status == 429
  end

  test "bug: buffer limiting is based on all queues", %{
    conn: conn,
    source: source,
    table_key: table_key
  } do
    other_table_key = {source.id, nil, make_ref()}
    IngestEventQueue.upsert_tid(other_table_key)

    for _ <- 1..25_100 do
      le = build(:log_event)
      IngestEventQueue.add_to_table(table_key, [le])
      IngestEventQueue.add_to_table(other_table_key, [le])
    end

    # get and cache the value
    Backends.cache_local_buffer_lens(source.id, nil)

    conn =
      conn
      |> assign(:source, source)
      |> BufferLimiter.call(%{})

    assert conn.halted == false

    for _ <- 1..25_100 do
      le = build(:log_event)
      IngestEventQueue.add_to_table(table_key, [le])
      IngestEventQueue.add_to_table(other_table_key, [le])
    end

    # get and cache the value
    Backends.cache_local_buffer_lens(source.id, nil)

    conn =
      conn
      |> recycle()
      |> assign(:source, source)
      |> BufferLimiter.call(%{})

    assert conn.halted == true
    assert conn.status == 429
  end

  test "200 if most events are ingested", %{conn: conn, source: source, table_key: table_key} do
    for _ <- 1..8_000 do
      le = build(:log_event)
      IngestEventQueue.add_to_table(table_key, [le])
      IngestEventQueue.mark_ingested(table_key, [le])
    end

    # get and cache the value
    Backends.cache_local_buffer_lens(source.id, nil)

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
