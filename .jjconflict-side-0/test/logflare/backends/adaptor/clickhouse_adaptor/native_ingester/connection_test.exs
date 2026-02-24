defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.ConnectionTest do
  @moduledoc false

  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.Connection

  @connect_opts [
    host: "localhost",
    port: 9000,
    database: "logflare_test",
    username: "logflare",
    password: "logflare"
  ]

  describe "connect/1" do
    test "connects and completes handshake with local ClickHouse" do
      assert {:ok, conn} = Connection.connect(@connect_opts)

      assert conn.host == "localhost"
      assert conn.port == 9000
      assert conn.database == "logflare_test"
      assert conn.username == "logflare"
      assert conn.transport == :gen_tcp
      assert conn.socket != nil
      assert conn.buffer == <<>>

      assert conn.server_info.name == "ClickHouse"
      assert conn.server_info.major > 0
      assert conn.server_info.minor >= 0
      assert conn.server_info.revision > 0
      assert is_binary(conn.server_info.timezone)
      assert is_binary(conn.server_info.display_name)
      assert is_integer(conn.server_info.patch)

      assert conn.negotiated_rev > 0
      assert conn.negotiated_rev <= 54_483

      assert :ok = Connection.close(conn)
    end

    test "raises when required options are missing" do
      assert_raise KeyError, ~r/:host/, fn ->
        Connection.connect(Keyword.delete(@connect_opts, :host))
      end

      assert_raise KeyError, ~r/:database/, fn ->
        Connection.connect(Keyword.delete(@connect_opts, :database))
      end

      assert_raise KeyError, ~r/:username/, fn ->
        Connection.connect(Keyword.delete(@connect_opts, :username))
      end

      assert_raise KeyError, ~r/:password/, fn ->
        Connection.connect(Keyword.delete(@connect_opts, :password))
      end
    end

    test "raises for invalid transport" do
      assert_raise ArgumentError, ~r/:transport/, fn ->
        Connection.connect(Keyword.put(@connect_opts, :transport, :udp))
      end
    end

    test "returns error for unreachable host" do
      opts = Keyword.put(@connect_opts, :port, 19_999)
      assert {:error, :econnrefused} = Connection.connect(opts)
    end

    test "returns error for bad credentials" do
      opts = Keyword.put(@connect_opts, :password, "wrong_password")
      assert {:error, {:exception, _code, message}} = Connection.connect(opts)
      assert message =~ "ClickHouse exception"
    end
  end

  describe "close/1" do
    test "closing a nil socket returns :ok" do
      conn = %Connection{socket: nil, transport: :gen_tcp}
      assert :ok = Connection.close(conn)
    end

    test "closing a connected socket returns :ok" do
      {:ok, conn} = Connection.connect(@connect_opts)
      assert :ok = Connection.close(conn)
    end
  end

  describe "ping/1" do
    test "returns {:ok, conn} for healthy connection" do
      {:ok, conn} = Connection.connect(@connect_opts)
      assert {:ok, conn} = Connection.ping(conn)
      assert conn.socket != nil
      assert :ok = Connection.close(conn)
    end
  end

  describe "alive?/1" do
    test "returns false for nil socket" do
      conn = %Connection{socket: nil, transport: :gen_tcp}
      refute Connection.alive?(conn)
    end

    test "returns true for healthy connection" do
      {:ok, conn} = Connection.connect(@connect_opts)
      assert Connection.alive?(conn)
      assert :ok = Connection.close(conn)
    end
  end

  describe "send_query/4" do
    setup do
      insert(:plan, name: "Free")
      {_source, backend, cleanup_fn} = setup_clickhouse_test()
      start_supervised!({ClickHouseAdaptor, backend})

      on_exit(fn ->
        for table <- ["native_test_basic", "native_test_lc"] do
          ClickHouseAdaptor.execute_ch_query(backend, "DROP TABLE IF EXISTS #{table}")
        end

        cleanup_fn.()
      end)

      [backend: backend]
    end

    test "returns column info for a successful INSERT query", %{backend: backend} do
      {:ok, _} =
        ClickHouseAdaptor.execute_ch_query(backend, """
        CREATE TABLE IF NOT EXISTS native_test_basic (
          id UInt64,
          name String,
          value Float64,
          ts DateTime
        ) ENGINE = MergeTree() ORDER BY id
        """)

      {:ok, conn} = Connection.connect(@connect_opts)

      assert {:ok, column_info, conn} =
               Connection.send_query(
                 conn,
                 "INSERT INTO native_test_basic (id, name, value, ts) VALUES"
               )

      assert column_info == [
               {"id", "UInt64"},
               {"name", "String"},
               {"value", "Float64"},
               {"ts", "DateTime"}
             ]

      assert :ok = Connection.close(conn)
    end

    test "returns exception for non-existent table" do
      {:ok, conn} = Connection.connect(@connect_opts)

      assert {:error, {:exception, _code, message}} =
               Connection.send_query(conn, "INSERT INTO non_existent_table_xyz (id) VALUES")

      assert message =~ "non_existent_table_xyz"
      Connection.close(conn)
    end

    test "returns exception for invalid SQL" do
      {:ok, conn} = Connection.connect(@connect_opts)

      assert {:error, {:exception, _code, _message}} =
               Connection.send_query(conn, "THIS IS NOT VALID SQL")

      Connection.close(conn)
    end

    test "LowCardinality columns are reported as their base type", %{backend: backend} do
      {:ok, _} =
        ClickHouseAdaptor.execute_ch_query(backend, """
        CREATE TABLE IF NOT EXISTS native_test_lc (
          id UInt64,
          status LowCardinality(String)
        ) ENGINE = MergeTree() ORDER BY id
        """)

      {:ok, conn} = Connection.connect(@connect_opts)

      assert {:ok, column_info, conn} =
               Connection.send_query(conn, "INSERT INTO native_test_lc (id, status) VALUES")

      assert column_info == [
               {"id", "UInt64"},
               {"status", "String"}
             ]

      assert :ok = Connection.close(conn)
    end
  end
end
