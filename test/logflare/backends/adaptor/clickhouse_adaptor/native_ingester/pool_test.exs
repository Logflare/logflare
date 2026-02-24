defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.PoolTest do
  @moduledoc false

  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.BlockEncoder
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.Connection
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.Pool

  @pool_connect_opts [
    host: "localhost",
    port: 9000,
    database: "logflare_test",
    username: "logflare",
    password: "logflare",
    compression: :lz4
  ]

  @test_tables [
    "pool_basic",
    "pool_insert",
    "pool_reuse"
  ]

  setup do
    insert(:plan, name: "Free")
    {_source, backend, cleanup_fn} = setup_clickhouse_test()
    start_supervised!({ClickHouseAdaptor, backend})

    on_exit(fn ->
      for table <- @test_tables do
        ClickHouseAdaptor.execute_ch_query(backend, "DROP TABLE IF EXISTS #{table}")
      end

      cleanup_fn.()
    end)

    [backend: backend]
  end

  describe "Pool lifecycle" do
    test "starts and stops a pool" do
      pool_name = :"pool_lifecycle_test_#{System.unique_integer([:positive])}"

      {:ok, pid} =
        NimblePool.start_link(
          worker: {Pool, @pool_connect_opts},
          pool_size: 2,
          name: pool_name
        )

      assert Process.alive?(pid)
      GenServer.stop(pid)
    end

    test "pool creates connections on checkout" do
      pool_name = :"pool_checkout_test_#{System.unique_integer([:positive])}"

      {:ok, pid} =
        NimblePool.start_link(
          worker: {Pool, @pool_connect_opts},
          pool_size: 1,
          name: pool_name
        )

      result =
        NimblePool.checkout!(pool_name, :checkout, fn _pool, conn ->
          assert %Connection{} = conn
          assert conn.server_info.name == "ClickHouse"
          {:ok, conn}
        end)

      assert result == :ok

      GenServer.stop(pid)
    end

    test "pool recycles dead connections", %{backend: backend} do
      {:ok, _} =
        ClickHouseAdaptor.execute_ch_query(backend, """
        CREATE TABLE IF NOT EXISTS pool_basic (
          id UInt64,
          name String
        ) ENGINE = MergeTree() ORDER BY id
        """)

      pool_name = :"pool_recycle_test_#{System.unique_integer([:positive])}"

      {:ok, pid} =
        NimblePool.start_link(
          worker: {Pool, @pool_connect_opts},
          pool_size: 1,
          name: pool_name
        )

      # First checkout: close socket to simulate disconnect
      NimblePool.checkout!(pool_name, :checkout, fn _pool, conn ->
        Connection.close(conn)
        {:ok, :remove}
      end)

      # Second checkout: should get a fresh connection (pool recreates worker)
      result =
        NimblePool.checkout!(pool_name, :checkout, fn _pool, conn ->
          assert %Connection{} = conn
          assert Connection.alive?(conn)

          case do_insert(conn, "pool_basic", [
                 {"id", "UInt64", [1]},
                 {"name", "String", ["after_recycle"]}
               ]) do
            {:ok, updated_conn} -> {:ok, updated_conn}
            {:error, _} = error -> {error, :remove}
          end
        end)

      assert result == :ok

      {:ok, rows} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          "SELECT id, name FROM pool_basic"
        )

      assert length(rows) == 1
      assert Enum.at(rows, 0)["name"] == "after_recycle"

      GenServer.stop(pid)
    end
  end

  describe "insert through pool" do
    test "checkout, insert, and checkin cycle works", %{backend: backend} do
      {:ok, _} =
        ClickHouseAdaptor.execute_ch_query(backend, """
        CREATE TABLE IF NOT EXISTS pool_insert (
          id UInt64,
          name String
        ) ENGINE = MergeTree() ORDER BY id
        """)

      pool_name = :"pool_insert_test_#{System.unique_integer([:positive])}"

      {:ok, pid} =
        NimblePool.start_link(
          worker: {Pool, @pool_connect_opts},
          pool_size: 1,
          name: pool_name
        )

      result =
        NimblePool.checkout!(pool_name, :checkout, fn _pool, conn ->
          case do_insert(conn, "pool_insert", [
                 {"id", "UInt64", [1, 2]},
                 {"name", "String", ["alice", "bob"]}
               ]) do
            {:ok, updated_conn} -> {:ok, updated_conn}
            {:error, _} = error -> {error, :remove}
          end
        end)

      assert result == :ok

      {:ok, rows} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          "SELECT id, name FROM pool_insert ORDER BY id"
        )

      assert length(rows) == 2
      assert Enum.at(rows, 0)["name"] == "alice"
      assert Enum.at(rows, 1)["name"] == "bob"

      GenServer.stop(pid)
    end
  end

  describe "handle_ping" do
    test "removes stale connections when socket is closed" do
      pool_name = :"pool_ping_stale_#{System.unique_integer([:positive])}"

      {:ok, pid} =
        NimblePool.start_link(
          worker: {Pool, @pool_connect_opts},
          pool_size: 1,
          lazy: false,
          worker_idle_timeout: 100,
          name: pool_name
        )

      # Check out the connection, close the socket, then check it back in
      NimblePool.checkout!(pool_name, :checkout, fn _pool, conn ->
        Connection.close(conn)
        {:ok, conn}
      end)

      # Wait for the idle timeout + ping cycle to fire and remove the stale worker
      Process.sleep(300)

      # Next checkout should get a fresh, healthy connection
      result =
        NimblePool.checkout!(pool_name, :checkout, fn _pool, conn ->
          assert %Connection{} = conn
          assert Connection.alive?(conn)
          {:ok, conn}
        end)

      assert result == :ok

      GenServer.stop(pid)
    end

    test "keeps healthy connections alive across ping cycles" do
      pool_name = :"pool_ping_healthy_#{System.unique_integer([:positive])}"

      {:ok, pid} =
        NimblePool.start_link(
          worker: {Pool, @pool_connect_opts},
          pool_size: 1,
          lazy: false,
          worker_idle_timeout: 100,
          name: pool_name
        )

      # Get the initial connection's connected_at
      initial_connected_at =
        NimblePool.checkout!(pool_name, :checkout, fn _pool, conn ->
          {conn.connected_at, conn}
        end)

      # Let several ping cycles pass
      Process.sleep(350)

      # Connection should still be the same (not replaced)
      final_connected_at =
        NimblePool.checkout!(pool_name, :checkout, fn _pool, conn ->
          assert Connection.alive?(conn)
          {conn.connected_at, conn}
        end)

      assert initial_connected_at == final_connected_at

      GenServer.stop(pid)
    end
  end

  describe "Pool connection reuse" do
    test "multiple inserts reuse pooled connections", %{backend: backend} do
      {:ok, _} =
        ClickHouseAdaptor.execute_ch_query(backend, """
        CREATE TABLE IF NOT EXISTS pool_reuse (
          id UInt64,
          name String
        ) ENGINE = MergeTree() ORDER BY id
        """)

      pool_name = :"pool_reuse_test_#{System.unique_integer([:positive])}"

      {:ok, pid} =
        NimblePool.start_link(
          worker: {Pool, @pool_connect_opts},
          pool_size: 1,
          name: pool_name
        )

      for i <- 1..3 do
        result =
          NimblePool.checkout!(pool_name, :checkout, fn _pool, conn ->
            case do_insert(conn, "pool_reuse", [
                   {"id", "UInt64", [i]},
                   {"name", "String", ["batch_#{i}"]}
                 ]) do
              {:ok, updated_conn} -> {:ok, updated_conn}
              {:error, _} = error -> {error, :remove}
            end
          end)

        assert result == :ok
      end

      {:ok, rows} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          "SELECT id, name FROM pool_reuse ORDER BY id"
        )

      assert length(rows) == 3
      assert Enum.map(rows, & &1["name"]) == ["batch_1", "batch_2", "batch_3"]

      GenServer.stop(pid)
    end
  end

  defp do_insert(conn, table, columns) do
    column_names = Enum.map(columns, &elem(&1, 0))
    cols = Enum.join(column_names, ", ")
    sql = "INSERT INTO #{conn.database}.#{table} (#{cols}) VALUES"

    with {:ok, _server_columns, conn} <- Connection.send_query(conn, sql, []) do
      body = BlockEncoder.encode_block_body(columns, conn.negotiated_rev)

      with :ok <- Connection.send_data_block(conn, body),
           :ok <- Connection.send_data_block(conn, BlockEncoder.encode_empty_block_body()),
           {:ok, conn} <- Connection.read_insert_response(conn) do
        {:ok, conn}
      end
    end
  end
end
