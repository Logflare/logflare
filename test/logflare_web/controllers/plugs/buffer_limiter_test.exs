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

    assert conn.halted
    assert json_response(conn, 429) == %{"error" => "Buffer Full: Too Many Requests"}
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

    assert conn.halted
    assert json_response(conn, 429)
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

      Backends.Cache.get_backend(backend1.id)
      Backends.Cache.get_backend(backend2.id)

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

      Backends.cache_local_buffer_lens(source.id, nil)

      conn =
        conn
        |> assign(:source, source)
        |> BufferLimiter.call(%{})

      assert conn.halted == false
    end

    test "returns 429 when default ingest backend is full", %{
      conn: conn,
      source: source
    } do
      table_key = {source.id, nil, self()}
      IngestEventQueue.upsert_tid(table_key)

      # Fill up the default ingest backend
      for _ <- 1..(Backends.max_buffer_queue_len() + 500) do
        le = build(:log_event)
        IngestEventQueue.add_to_table(table_key, [le])
      end

      Backends.cache_local_buffer_lens(source.id, nil)

      conn =
        conn
        |> assign(:source, source)
        |> BufferLimiter.call(%{})

      assert conn.halted
      assert json_response(conn, 429)
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

      assert conn.halted
      assert json_response(conn, 429)
    end

    test "returns 429 when system default queue is full even if user defaults are not", %{
      conn: conn,
      source: source,
      backend1: backend1
    } do
      system_queue_key = {source.id, nil, self()}
      IngestEventQueue.upsert_tid(system_queue_key)

      for _ <- 1..(Backends.max_buffer_queue_len() + 500) do
        le = build(:log_event)
        IngestEventQueue.add_to_table(system_queue_key, [le])
      end

      user_queue_key = {source.id, backend1.id, self()}
      IngestEventQueue.upsert_tid(user_queue_key)

      for _ <- 1..100 do
        le = build(:log_event)
        IngestEventQueue.add_to_table(user_queue_key, [le])
      end

      Backends.cache_local_buffer_lens(source.id, nil)
      Backends.cache_local_buffer_lens(source.id, backend1.id)

      conn =
        conn
        |> assign(:source, source)
        |> BufferLimiter.call(%{})

      assert conn.halted
      assert json_response(conn, 429)
    end

    test "returns 429 when user's backend default ingest queue is full even if system queue is not",
         %{
           conn: conn,
           source: source,
           backend1: backend1
         } do
      system_queue_key = {source.id, nil, spawn(fn -> :ok end)}
      IngestEventQueue.upsert_tid(system_queue_key)

      for _ <- 1..100 do
        le = build(:log_event)
        IngestEventQueue.add_to_table(system_queue_key, [le])
      end

      user_queue_key = {source.id, backend1.id, spawn(fn -> :ok end)}
      IngestEventQueue.upsert_tid(user_queue_key)

      for _ <- 1..(Backends.max_buffer_queue_len() + 500) do
        le = build(:log_event)
        IngestEventQueue.add_to_table(user_queue_key, [le])
      end

      Backends.cache_local_buffer_lens(source.id, nil)
      Backends.cache_local_buffer_lens(source.id, backend1.id)

      conn =
        conn
        |> assign(:source, source)
        |> BufferLimiter.call(%{})

      assert conn.halted
      assert json_response(conn, 429)
    end

    test "allows request when both system and default ingest buffers have space", %{
      conn: conn,
      source: source,
      backend1: backend1
    } do
      system_queue_key = {source.id, nil, self()}
      IngestEventQueue.upsert_tid(system_queue_key)

      for _ <- 1..100 do
        le = build(:log_event)
        IngestEventQueue.add_to_table(system_queue_key, [le])
      end

      user_queue_key = {source.id, backend1.id, self()}
      IngestEventQueue.upsert_tid(user_queue_key)

      for _ <- 1..100 do
        le = build(:log_event)
        IngestEventQueue.add_to_table(user_queue_key, [le])
      end

      Backends.cache_local_buffer_lens(source.id, nil)
      Backends.cache_local_buffer_lens(source.id, backend1.id)

      conn =
        conn
        |> assign(:source, source)
        |> BufferLimiter.call(%{})

      refute conn.halted
    end

    test "ignores unlinked backend buffers even if backend has default ingest enabled", %{
      conn: conn
    } do
      user = insert(:user)
      source = insert(:source, user: user, default_ingest_backend_enabled?: true)

      unlinked_backend =
        insert(:backend,
          user: user,
          type: :bigquery,
          config: %{project_id: "test", dataset_id: "test"},
          default_ingest?: true
        )

      unlinked_queue_key = {source.id, unlinked_backend.id, self()}
      IngestEventQueue.upsert_tid(unlinked_queue_key)

      for _ <- 1..(Backends.max_buffer_queue_len() + 500) do
        le = build(:log_event)
        IngestEventQueue.add_to_table(unlinked_queue_key, [le])
      end

      Backends.cache_local_buffer_lens(source.id, unlinked_backend.id)

      conn =
        conn
        |> assign(:source, source)
        |> BufferLimiter.call(%{})

      assert conn.halted == false
    end

    test "returns 429 when system buffer is full, even if source has default ingest disabled", %{
      conn: conn
    } do
      user = insert(:user)
      source = insert(:source, user: user, default_ingest_backend_enabled?: false)

      backend =
        insert(:backend,
          user: user,
          type: :bigquery,
          config: %{project_id: "test", dataset_id: "test"},
          default_ingest?: true
        )

      {:ok, _} = Backends.update_source_backends(source, [backend])

      backend_queue_key = {source.id, backend.id, self()}
      IngestEventQueue.upsert_tid(backend_queue_key)

      for _ <- 1..(Backends.max_buffer_queue_len() + 500) do
        le = build(:log_event)
        IngestEventQueue.add_to_table(backend_queue_key, [le])
      end

      Backends.cache_local_buffer_lens(source.id, backend.id)

      other_queue_key = {source.id, nil, self()}
      IngestEventQueue.upsert_tid(other_queue_key)

      for _ <- 1..(Backends.max_buffer_queue_len() + 500) do
        le = build(:log_event)
        IngestEventQueue.add_to_table(other_queue_key, [le])
      end

      Backends.cache_local_buffer_lens(source.id, nil)

      conn =
        conn
        |> assign(:source, source)
        |> BufferLimiter.call(%{})

      assert conn.halted
      assert json_response(conn, 429)
    end

    test "ignores backend buffers when source has default ingest disabled", %{
      conn: conn
    } do
      user = insert(:user)
      source = insert(:source, user: user, default_ingest_backend_enabled?: false)

      backend =
        insert(:backend,
          user: user,
          type: :bigquery,
          config: %{project_id: "test", dataset_id: "test"},
          default_ingest?: true
        )

      {:ok, _} = Backends.update_source_backends(source, [backend])

      backend_queue_key = {source.id, backend.id, self()}
      IngestEventQueue.upsert_tid(backend_queue_key)

      for _ <- 1..(Backends.max_buffer_queue_len() + 500) do
        le = build(:log_event)
        IngestEventQueue.add_to_table(backend_queue_key, [le])
      end

      Backends.cache_local_buffer_lens(source.id, backend.id)

      conn =
        conn
        |> assign(:source, source)
        |> BufferLimiter.call(%{})

      refute conn.halted
    end

    test "returns 429 when user-configured ClickHouse default backend is full but system default is not",
         %{conn: conn} do
      user = insert(:user)
      source = insert(:source, user: user, default_ingest_backend_enabled?: true)

      clickhouse_backend =
        insert(:backend,
          user: user,
          type: :clickhouse,
          config: %{
            url: "http://localhost:8123",
            username: "default",
            password: "",
            database: "test_db"
          },
          default_ingest?: true
        )

      {:ok, _} = Backends.update_source_backends(source, [clickhouse_backend])

      # Keep system default queue under the limit
      system_queue_key = {source.id, nil, self()}
      IngestEventQueue.upsert_tid(system_queue_key)

      for _ <- 1..100 do
        le = build(:log_event)
        IngestEventQueue.add_to_table(system_queue_key, [le])
      end

      # Fill CH backend queue over the limit
      clickhouse_queue_key = {source.id, clickhouse_backend.id, self()}
      IngestEventQueue.upsert_tid(clickhouse_queue_key)

      for _ <- 1..(Backends.max_buffer_queue_len() + 500) do
        le = build(:log_event)
        IngestEventQueue.add_to_table(clickhouse_queue_key, [le])
      end

      # Cache buffer stats for both backends
      Backends.cache_local_buffer_lens(source.id, nil)
      Backends.cache_local_buffer_lens(source.id, clickhouse_backend.id)

      conn =
        conn
        |> assign(:source, source)
        |> BufferLimiter.call(%{})

      assert conn.halted
      assert json_response(conn, 429)
    end
  end
end
