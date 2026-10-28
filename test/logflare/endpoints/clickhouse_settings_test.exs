defmodule Logflare.Endpoints.ClickHouseSettingsTest do
  use ExUnit.Case, async: true

  alias Logflare.Endpoints.ClickHouseSettings
  alias Logflare.Sql.Parser

  test "normalizes positive limits and forces read overflow to throw" do
    assert {:ok, %{"max_bytes_to_read" => 100, "read_overflow_mode" => "throw"}} =
             ClickHouseSettings.normalize(%{"max_bytes_to_read" => 100})

    for settings <- [
          %{"max_bytes_to_read" => 0},
          %{"max_memory_usage" => "100"},
          %{"read_overflow_mode" => "break"},
          %{"allow_introspection_functions" => 1},
          %{"max_execution_time" => -1}
        ] do
      assert {:error, _} = ClickHouseSettings.normalize(settings)
    end
  end

  test "native parameters retain their bindings without bypassing nested conflicts" do
    sql = "WITH data AS (SELECT {$0:Int64} AS n) SELECT n, '{$0:Int64}' AS literal FROM data"
    assert {:ok, prepared} = ClickHouseSettings.enforce(sql, %{"priority" => 5})
    assert prepared =~ "{$0: Int64}"
    assert prepared =~ "'{$0:Int64}' AS literal"
    assert prepared =~ "priority = 5"

    assert {:error, reason} =
             ClickHouseSettings.enforce(
               "WITH data AS (SELECT {$0:Int64} AS n SETTINGS priority = 1) SELECT n FROM data",
               %{"priority" => 5}
             )

    assert reason =~ "priority is enforced"
  end

  test "empty policy leaves SQL untouched without parsing" do
    query = "not SQL; {$0:Int64}"
    assert {:ok, ^query} = ClickHouseSettings.enforce(query, %{})
  end

  test "round-trips numeric bounds and overflow literals through the native serializer" do
    for value <- [1, 9_223_372_036_854_775_807] do
      settings = %{
        "max_bytes_to_read" => value,
        "max_rows_to_read" => value,
        "max_memory_usage" => value,
        "max_execution_time" => value,
        "max_threads" => value,
        "priority" => value
      }

      assert {:ok, sql} = ClickHouseSettings.enforce("SELECT a FROM t", settings)

      assert sql ==
               "SELECT a FROM t SETTINGS max_bytes_to_read = #{value}, " <>
                 "max_execution_time = #{value}, max_memory_usage = #{value}, " <>
                 "max_rows_to_read = #{value}, max_threads = #{value}, priority = #{value}, " <>
                 "read_overflow_mode = 'throw', timeout_overflow_mode = 'throw'"

      assert {:ok, [_statement]} = Parser.parse("clickhouse", sql)
    end
  end

  test "rejects untrusted setting keys and literal types before AST construction" do
    for settings <- [
          %{"max_memory_usage" => "100; SELECT 1"},
          %{"read_overflow_mode" => "throw' SETTINGS priority = 1"},
          %{"max_memory_usage = 1, priority" => 1}
        ] do
      assert {:error, _reason} = ClickHouseSettings.enforce("SELECT a FROM t", settings)
    end
  end

  test "round-trips fractional timeout literals and their overflow mode" do
    for value <- [0.25, 1.0, 1.0e-6, 1.0e15] do
      assert {:ok, sql} =
               ClickHouseSettings.enforce("SELECT a FROM t", %{"max_execution_time" => value})

      assert sql ==
               "SELECT a FROM t SETTINGS max_execution_time = #{value}, " <>
                 "timeout_overflow_mode = 'throw'"

      assert {:ok, [_statement]} = Parser.parse("clickhouse", sql)
    end
  end

  test "validated SQL without policy syntax avoids the AST round trip" do
    for query <- [
          "SELECT  a FROM t",
          "WITH data AS (SELECT {$0:Int64} AS n) SELECT n FROM data",
          "EXPLAIN ANALYZE SELECT a FROM t",
          "SELECT a FROM t UNION ALL SELECT b FROM s"
        ] do
      assert {:ok, prepared} = ClickHouseSettings.enforce_validated(query, %{"priority" => 5})
      assert prepared == query <> " SETTINGS priority = 5"
      assert {:ok, ast} = Parser.parse_clickhouse_converted(prepared)
      assert {:ok, canonical_fast} = Parser.to_string(ast)
      assert {:ok, ^canonical_fast} = ClickHouseSettings.enforce(query, %{"priority" => 5})
    end
  end

  test "validated enforcement retains AST checks for conflicting or ambiguous syntax" do
    for query <- [
          "SELECT a FROM t SETTINGS PRIORITY = 0",
          "WITH data AS (SELECT a FROM t SETTINGS priority = 0) SELECT a FROM data",
          "EXPLAIN SELECT a FROM t SETTINGS priority = 0",
          "SELECT 1; SELECT 2",
          "EXPLAIN TABLE t"
        ] do
      assert {:error, _} = ClickHouseSettings.enforce_validated(query, %{"priority" => 5})
    end

    for query <- [
          "SELECT 1 -- trailing comment",
          "SELECT 1 /* comment */",
          "SELECT 1;",
          "SELECT a FROM t SETTINGS optimize_read_in_order = 1"
        ] do
      assert ClickHouseSettings.enforce_validated(query, %{"priority" => 5}) ==
               ClickHouseSettings.enforce(query, %{"priority" => 5})
    end

    assert {:error, _} =
             ClickHouseSettings.enforce_validated("SELECT 1", %{"priority" => "5; SELECT 2"})
  end

  test "validated policy literals match AST enforcement at numeric bounds" do
    for limit <- [1, 9_223_372_036_854_775_807, 0.25, 1.0e-6] do
      settings = %{"max_execution_time" => limit, "max_rows_to_read" => 1, "priority" => 5}

      assert ClickHouseSettings.enforce_validated("SELECT a FROM t", settings) ==
               ClickHouseSettings.enforce("SELECT a FROM t", settings)
    end
  end

  test "preserves caller settings and adds resource limits to the final query" do
    query = "WITH src AS (SELECT a FROM t) SELECT a FROM src SETTINGS optimize_read_in_order = 1"
    settings = %{"max_execution_time" => 5, "max_memory_usage" => 100}

    assert {:ok, transformed} = ClickHouseSettings.enforce(query, settings)
    assert transformed =~ "WITH src AS (SELECT a FROM t)"
    assert transformed =~ "optimize_read_in_order = 1"
    assert transformed =~ "max_execution_time = 5"
    assert transformed =~ "max_memory_usage = 100"
  end

  test "preserves EXPLAIN options and enforces settings on the explained query" do
    for prefix <- ["EXPLAIN", "EXPLAIN ANALYZE"] do
      query = "#{prefix} SELECT a FROM t SETTINGS optimize_read_in_order = 1"
      assert {:ok, [%{"Explain" => original}]} = Parser.parse("clickhouse", query)

      assert {:ok, transformed} =
               ClickHouseSettings.enforce(query, %{"max_execution_time" => 5})

      assert String.starts_with?(transformed, prefix)
      assert transformed =~ "optimize_read_in_order = 1"
      assert transformed =~ "max_execution_time = 5"
      assert {:ok, [%{"Explain" => explained}]} = Parser.parse("clickhouse", transformed)
      assert Map.delete(explained, "statement") == Map.delete(original, "statement")
    end
  end

  test "still rejects non-query statements and multiple statements" do
    for query <- ["EXPLAIN TABLE t", "EXPLAIN INSERT INTO t VALUES (1)", "SELECT 1; SELECT 2"] do
      assert {:error, "Expected one ClickHouse SELECT or EXPLAIN SELECT query"} =
               ClickHouseSettings.enforce(query, %{"max_execution_time" => 5})
    end
  end

  test "preserves FINAL when enforcing limits" do
    assert {:ok, sql} =
             ClickHouseSettings.enforce("SELECT a FROM t FINAL WHERE a > 0", %{
               "max_execution_time" => 5
             })

    assert sql =~ "FROM t FINAL WHERE"
    refute sql =~ "AS FINAL"
  end

  test "rejects conflicting settings in the outer query or a nested query" do
    settings = %{"max_execution_time" => 5}

    for query <- [
          "SELECT a FROM t SETTINGS max_execution_time = 10",
          "WITH src AS (SELECT a FROM t SETTINGS max_execution_time = 10) SELECT a FROM src",
          "EXPLAIN SELECT a FROM t SETTINGS max_execution_time = 10",
          "EXPLAIN WITH src AS (SELECT a FROM t SETTINGS max_execution_time = 10) SELECT a FROM src"
        ] do
      assert {:error, "ClickHouse setting max_execution_time is enforced by query policy"} =
               ClickHouseSettings.enforce(query, settings)
    end
  end
end
