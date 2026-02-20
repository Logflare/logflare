defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngesterTest do
  @moduledoc false

  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.Connection

  @connect_opts [
    host: "localhost",
    port: 9000,
    database: "logflare_test",
    username: "logflare",
    password: "logflare"
  ]

  @test_tables [
    "native_insert_basic",
    "native_insert_types",
    "native_insert_large",
    "native_insert_reuse"
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

  describe "insert/4" do
    test "inserts basic types and verifies data via HTTP", %{backend: backend} do
      {:ok, _} =
        ClickHouseAdaptor.execute_ch_query(backend, """
        CREATE TABLE IF NOT EXISTS native_insert_basic (
          id UInt64,
          name String,
          value Float64,
          active Bool
        ) ENGINE = MergeTree() ORDER BY id
        """)

      {:ok, conn} = Connection.connect(@connect_opts)

      columns = [
        {"id", "UInt64", [1, 2, 3]},
        {"name", "String", ["alice", "bob", "charlie"]},
        {"value", "Float64", [1.5, 2.7, 3.14]},
        {"active", "Bool", [true, false, true]}
      ]

      assert {:ok, conn} = NativeIngester.insert(conn, "native_insert_basic", columns)

      # Verify data via HTTP read
      {:ok, rows} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          "SELECT id, name, value, active FROM native_insert_basic ORDER BY id"
        )

      assert length(rows) == 3

      assert Enum.at(rows, 0)["id"] == 1
      assert Enum.at(rows, 0)["name"] == "alice"
      assert_in_delta Enum.at(rows, 0)["value"], 1.5, 0.001
      assert Enum.at(rows, 0)["active"] == true

      assert Enum.at(rows, 1)["id"] == 2
      assert Enum.at(rows, 1)["name"] == "bob"
      assert_in_delta Enum.at(rows, 1)["value"], 2.7, 0.001
      assert Enum.at(rows, 1)["active"] == false

      assert Enum.at(rows, 2)["id"] == 3
      assert Enum.at(rows, 2)["name"] == "charlie"
      assert_in_delta Enum.at(rows, 2)["value"], 3.14, 0.001
      assert Enum.at(rows, 2)["active"] == true

      assert :ok = Connection.close(conn)
    end

    test "inserts DateTime, DateTime64, and UUID types", %{backend: backend} do
      {:ok, _} =
        ClickHouseAdaptor.execute_ch_query(backend, """
        CREATE TABLE IF NOT EXISTS native_insert_types (
          id UInt64,
          uuid UUID,
          ts DateTime,
          ts_precise DateTime64(9)
        ) ENGINE = MergeTree() ORDER BY id
        """)

      {:ok, conn} = Connection.connect(@connect_opts)

      uuid1 = Ecto.UUID.generate()
      uuid2 = Ecto.UUID.generate()
      {:ok, uuid1_raw} = Ecto.UUID.dump(uuid1)
      {:ok, uuid2_raw} = Ecto.UUID.dump(uuid2)

      # DateTime as Unix epoch seconds
      ts1 = 1_700_000_000
      ts2 = 1_700_000_060

      # DateTime64(9) as nanoseconds since epoch
      ts64_1 = 1_700_000_000_123_456_789
      ts64_2 = 1_700_000_060_987_654_321

      columns = [
        {"id", "UInt64", [1, 2]},
        {"uuid", "UUID", [uuid1_raw, uuid2_raw]},
        {"ts", "DateTime", [ts1, ts2]},
        {"ts_precise", "DateTime64(9)", [ts64_1, ts64_2]}
      ]

      assert {:ok, conn} = NativeIngester.insert(conn, "native_insert_types", columns)

      {:ok, rows} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          "SELECT id, toString(uuid) as uuid_str, toUnixTimestamp(ts) as ts FROM native_insert_types ORDER BY id"
        )

      assert length(rows) == 2

      assert Enum.at(rows, 0)["id"] == 1
      assert String.downcase(Enum.at(rows, 0)["uuid_str"]) == String.downcase(uuid1)
      assert Enum.at(rows, 0)["ts"] == ts1

      assert Enum.at(rows, 1)["id"] == 2
      assert String.downcase(Enum.at(rows, 1)["uuid_str"]) == String.downcase(uuid2)
      assert Enum.at(rows, 1)["ts"] == ts2

      # Verify DateTime64 precision separately with nanosecond extraction
      {:ok, ts_rows} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          "SELECT id, toUnixTimestamp64Nano(ts_precise) as ts_ns FROM native_insert_types ORDER BY id"
        )

      assert Enum.at(ts_rows, 0)["ts_ns"] == ts64_1
      assert Enum.at(ts_rows, 1)["ts_ns"] == ts64_2

      assert :ok = Connection.close(conn)
    end

    test "returns exception when column name does not exist", %{backend: backend} do
      {:ok, _} =
        ClickHouseAdaptor.execute_ch_query(backend, """
        CREATE TABLE IF NOT EXISTS native_insert_basic (
          id UInt64,
          name String,
          value Float64,
          active Bool
        ) ENGINE = MergeTree() ORDER BY id
        """)

      {:ok, conn} = Connection.connect(@connect_opts)

      # Wrong column name: "wrong_name" doesn't exist in the table
      # ClickHouse rejects this at the SQL level before our validation runs
      columns = [
        {"id", "UInt64", [1]},
        {"wrong_name", "String", ["test"]},
        {"value", "Float64", [1.0]},
        {"active", "Bool", [true]}
      ]

      assert {:error, {:exception, _code, message}} =
               NativeIngester.insert(conn, "native_insert_basic", columns)

      assert message =~ "wrong_name"

      Connection.close(conn)
    end

    test "returns column mismatch error when column types differ", %{backend: backend} do
      {:ok, _} =
        ClickHouseAdaptor.execute_ch_query(backend, """
        CREATE TABLE IF NOT EXISTS native_insert_basic (
          id UInt64,
          name String,
          value Float64,
          active Bool
        ) ENGINE = MergeTree() ORDER BY id
        """)

      {:ok, conn} = Connection.connect(@connect_opts)

      # Wrong type: "Int32" instead of "Float64" for value
      columns = [
        {"id", "UInt64", [1]},
        {"name", "String", ["test"]},
        {"value", "Int32", [1]},
        {"active", "Bool", [true]}
      ]

      assert {:error, {:column_mismatch, opts}} =
               NativeIngester.insert(conn, "native_insert_basic", columns)

      assert Keyword.get(opts, :got) == [
               {"id", "UInt64"},
               {"name", "String"},
               {"value", "Int32"},
               {"active", "Bool"}
             ]

      Connection.close(conn)
    end

    test "inserts large batch with sub-block splitting", %{backend: backend} do
      {:ok, _} =
        ClickHouseAdaptor.execute_ch_query(backend, """
        CREATE TABLE IF NOT EXISTS native_insert_large (
          id UInt64,
          name String,
          value Float64
        ) ENGINE = MergeTree() ORDER BY id
        """)

      {:ok, conn} = Connection.connect(@connect_opts)

      # 25K rows — triggers sub-block splitting (threshold is 10K)
      num_rows = 25_000

      columns = [
        {"id", "UInt64", Enum.to_list(1..num_rows)},
        {"name", "String", Enum.map(1..num_rows, &"row_#{&1}")},
        {"value", "Float64", Enum.map(1..num_rows, &(&1 * 0.1))}
      ]

      assert {:ok, conn} = NativeIngester.insert(conn, "native_insert_large", columns)

      # Verify row count via HTTP
      {:ok, rows} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          "SELECT count() as cnt FROM native_insert_large"
        )

      assert Enum.at(rows, 0)["cnt"] == num_rows

      # Spot-check first and last rows
      {:ok, rows} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          "SELECT id, name, value FROM native_insert_large ORDER BY id LIMIT 1"
        )

      assert Enum.at(rows, 0)["id"] == 1
      assert Enum.at(rows, 0)["name"] == "row_1"

      {:ok, rows} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          "SELECT id, name, value FROM native_insert_large ORDER BY id DESC LIMIT 1"
        )

      assert Enum.at(rows, 0)["id"] == num_rows
      assert Enum.at(rows, 0)["name"] == "row_#{num_rows}"

      assert :ok = Connection.close(conn)
    end

    test "returns exception for non-existent table" do
      {:ok, conn} = Connection.connect(@connect_opts)

      columns = [{"id", "UInt64", [1]}]

      assert {:error, {:exception, _code, message}} =
               NativeIngester.insert(conn, "non_existent_table_xyz", columns)

      assert message =~ "non_existent_table_xyz"

      Connection.close(conn)
    end

    test "supports multiple inserts on the same connection", %{backend: backend} do
      {:ok, _} =
        ClickHouseAdaptor.execute_ch_query(backend, """
        CREATE TABLE IF NOT EXISTS native_insert_reuse (
          id UInt64,
          name String
        ) ENGINE = MergeTree() ORDER BY id
        """)

      {:ok, conn} = Connection.connect(@connect_opts)

      # First insert
      columns1 = [
        {"id", "UInt64", [1, 2]},
        {"name", "String", ["first", "second"]}
      ]

      assert {:ok, conn} = NativeIngester.insert(conn, "native_insert_reuse", columns1)

      # Second insert on the same connection
      columns2 = [
        {"id", "UInt64", [3, 4]},
        {"name", "String", ["third", "fourth"]}
      ]

      assert {:ok, conn} = NativeIngester.insert(conn, "native_insert_reuse", columns2)

      # Third insert
      columns3 = [
        {"id", "UInt64", [5]},
        {"name", "String", ["fifth"]}
      ]

      assert {:ok, conn} = NativeIngester.insert(conn, "native_insert_reuse", columns3)

      # Verify all 5 rows landed
      {:ok, rows} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          "SELECT id, name FROM native_insert_reuse ORDER BY id"
        )

      assert length(rows) == 5
      assert Enum.map(rows, & &1["id"]) == [1, 2, 3, 4, 5]
      assert Enum.map(rows, & &1["name"]) == ["first", "second", "third", "fourth", "fifth"]

      assert :ok = Connection.close(conn)
    end
  end
end
