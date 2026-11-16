defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.QueryClassMaintenanceTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Backend

  @backend %Backend{config: %{query_class_settings: %{"default" => %{"priority" => 5}}}}
  @maintenance [
    "SYSTEM FLUSH LOGS",
    "EXISTS TABLE system.query_log",
    "CREATE TABLE t (n UInt64) ENGINE = Memory",
    "DROP TABLE IF EXISTS t",
    "  show tables;"
  ]

  test "recognized raw maintenance statements are unchanged under class policy" do
    for sql <- @maintenance do
      assert ClickHouseAdaptor.prepare_query(@backend, sql) == {:ok, sql}
    end
  end

  test "read queries remain policy-controlled regardless of maintenance words or comments" do
    for sql <- [
          "SELECT 1",
          "WITH t AS (SELECT 1 AS n) SELECT n FROM t",
          "EXPLAIN SELECT 1",
          "/* SYSTEM FLUSH LOGS */ SELECT 1",
          "-- CREATE TABLE t\nSELECT 1",
          "SELECT 'DROP TABLE t' AS message"
        ] do
      assert {:ok, prepared} = ClickHouseAdaptor.prepare_query(@backend, sql)
      assert prepared =~ "priority = 5"
    end

    assert {:error, _} =
             ClickHouseAdaptor.prepare_query(@backend, "SELECT 1 SETTINGS priority = 0")
  end

  test "endpoint and validated paths do not gain a maintenance bypass" do
    for sql <- @maintenance,
        opts <- [
          [query_type: :endpoint],
          [validated_clickhouse_sql: true],
          [enforced_clickhouse_settings: %{"max_threads" => 2}]
        ] do
      assert {:error, _} = ClickHouseAdaptor.prepare_query(@backend, sql, opts)
    end
  end

  test "unknown syntax and multiple statements are not maintenance passthrough" do
    for sql <- [
          "not SQL",
          "SYSTEM FLUSH LOGS; SELECT 1 SETTINGS priority = 0",
          "CREATE TABLE t (n UInt64) ENGINE = Memory; SELECT 1 SETTINGS priority = 0"
        ] do
      assert {:error, _} = ClickHouseAdaptor.prepare_query(@backend, sql)
    end
  end
end
