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
