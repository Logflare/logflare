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
    for _ <- 1..(Backends.max_buffer_queue_len() + 500) do
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
    other_table_key = {source.id, nil, self()}
    IngestEventQueue.upsert_tid(other_table_key)

    for _ <- 1..round(Backends.max_buffer_queue_len() / 2) do
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
    for _ <- 1..(Backends.max_buffer_queue_len() - 500) do
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

  describe "default ingest feature" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user, default_ingest_backend_enabled?: true)

      backend1 =
        insert(:backend,
          user: user,
          type: :bigquery,
          config: %{project_id: "test", dataset_id: "test"},
          default_ingest?: true
        )

      backend2 =
        insert(:backend,
          user: user,
          type: :webhook,
          config: %{url: "http://test.com"},
          default_ingest?: false
        )

      {:ok, _} = Backends.update_source_backends(source, [backend1, backend2])

      conn = build_conn(:post, "/api/logs", %{"message" => "some text"})

      {:ok, conn: conn, source: source, backend1: backend1, backend2: backend2}
    end

    test "returns 429 only when default ingest backend is full", %{
      conn: conn,
      source: source,
      backend1: backend1,
      backend2: backend2
    } do
      table_key_webhook = {source.id, backend2.id, self()}
      IngestEventQueue.upsert_tid(table_key_webhook)

      for _ <- 1..(Backends.max_buffer_queue_len() + 500) do
        le = build(:log_event)
        IngestEventQueue.add_to_table(table_key_webhook, [le])
      end

      table_key_bigquery = {source.id, backend1.id, self()}
      IngestEventQueue.upsert_tid(table_key_bigquery)

      for _ <- 1..100 do
        le = build(:log_event)
        IngestEventQueue.add_to_table(table_key_bigquery, [le])
      end

      Backends.cache_local_buffer_lens(source.id, backend1.id)
      Backends.cache_local_buffer_lens(source.id, backend2.id)

      conn =
        conn
        |> assign(:source, source)
        |> BufferLimiter.call(%{})

      assert conn.halted == false
    end

    test "returns 429 when default ingest backend is full", %{
      conn: conn,
      source: source,
      backend1: backend1
    } do
      table_key = {source.id, backend1.id, self()}
      IngestEventQueue.upsert_tid(table_key)

      # Fill up the default ingest backend
      for _ <- 1..(Backends.max_buffer_queue_len() + 500) do
        le = build(:log_event)
        IngestEventQueue.add_to_table(table_key, [le])
      end

      Backends.cache_local_buffer_lens(source.id, backend1.id)

      conn =
        conn
        |> assign(:source, source)
        |> BufferLimiter.call(%{})

      assert conn.halted == true
      assert conn.status == 429
    end

    test "falls back to regular buffer check when default_ingest_backend_enabled? is false", %{
      conn: conn
    } do
      source = insert(:source, user: insert(:user), default_ingest_backend_enabled?: false)

      table_key = {source.id, nil, self()}
      IngestEventQueue.upsert_tid(table_key)

      for _ <- 1..(Backends.max_buffer_queue_len() + 500) do
        le = build(:log_event)
        IngestEventQueue.add_to_table(table_key, [le])
      end

      Backends.cache_local_buffer_lens(source.id, nil)

      conn =
        conn
        |> assign(:source, source)
        |> BufferLimiter.call(%{})

      assert conn.halted == true
      assert conn.status == 429
    end
  end
end
