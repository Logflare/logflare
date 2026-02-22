defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.PoolTest do
  @moduledoc false

  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester
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

          columns = [
            {"id", "UInt64", [1]},
            {"name", "String", ["after_recycle"]}
          ]

          case NativeIngester.insert(conn, "pool_basic", columns, []) do
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
          columns = [
            {"id", "UInt64", [1, 2]},
            {"name", "String", ["alice", "bob"]}
          ]

          case NativeIngester.insert(conn, "pool_insert", columns, []) do
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
            columns = [
              {"id", "UInt64", [i]},
              {"name", "String", ["batch_#{i}"]}
            ]

            case NativeIngester.insert(conn, "pool_reuse", columns, []) do
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
end
