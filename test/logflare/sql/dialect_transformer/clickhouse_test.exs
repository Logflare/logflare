defmodule Logflare.Sql.DialectTransformer.ClickHouseTest do
  use Logflare.DataCase

  alias Logflare.Sql.DialectTransformer.ClickHouse

  describe "quote_style/0" do
    test "returns nil for ClickHouse" do
      assert ClickHouse.quote_style() == nil
    end
  end

  describe "dialect/0" do
    test "returns clickhouse string" do
      assert ClickHouse.dialect() == "clickhouse"
    end
  end

  describe "transform_source_name/2" do
    test "passes through the source name unchanged" do
      data = %{sources: [], dialect: "clickhouse"}

      assert ClickHouse.transform_source_name("my_table", data) == "my_table"
    end
  end

  describe "apply_limit/2" do
    test "adds a top-level limit" do
      assert {:ok, "SELECT number FROM numbers(100) LIMIT 10"} =
               ClickHouse.apply_limit("SELECT number FROM numbers(100)", 10)
    end

    test "preserves a stricter existing limit and its offset" do
      assert {:ok, "SELECT number FROM numbers(100) LIMIT least(5, 10) OFFSET 2"} =
               ClickHouse.apply_limit(
                 "SELECT number FROM numbers(100) LIMIT 5 OFFSET 2",
                 10
               )
    end

    test "caps a larger existing limit" do
      assert {:ok, "SELECT number FROM numbers(100) LIMIT least(50, 10)"} =
               ClickHouse.apply_limit("SELECT number FROM numbers(100) LIMIT 50", 10)
    end

    test "wraps set operations before adding a global limit" do
      query = "SELECT number FROM numbers(5) UNION ALL SELECT number + 5 FROM numbers(5)"
      expected = "SELECT * FROM (#{query}) LIMIT 3"

      assert {:ok, ^expected} = ClickHouse.apply_limit(query, 3)
    end

    test "adds a global limit after LIMIT BY" do
      query = "SELECT number FROM numbers(100) LIMIT 1 BY number"

      assert {:ok, "SELECT * FROM (SELECT number FROM numbers(100) LIMIT 1 BY number) LIMIT 3"} =
               ClickHouse.apply_limit(query, 3)
    end

    test "caps negative limits after applying their end-relative semantics" do
      query = "SELECT number FROM numbers(100) ORDER BY number LIMIT -50"

      assert {:ok,
              "SELECT * FROM (SELECT number FROM numbers(100) ORDER BY number LIMIT -50) LIMIT 10"} =
               ClickHouse.apply_limit(query, 10)
    end

    test "caps fractional limits after applying their proportional semantics" do
      query = "SELECT number FROM numbers(100) ORDER BY number LIMIT 0.5"

      assert {:ok,
              "SELECT * FROM (SELECT number FROM numbers(100) ORDER BY number LIMIT 0.5) LIMIT 10"} =
               ClickHouse.apply_limit(query, 10)
    end

    test "caps arbitrary limit expressions after evaluating them" do
      query = "SELECT number FROM numbers(100) LIMIT least(50, 100)"

      assert {:ok,
              "SELECT * FROM (SELECT number FROM numbers(100) LIMIT least(50, 100)) LIMIT 10"} =
               ClickHouse.apply_limit(query, 10)
    end

    test "preserves a single read-only non-query statement" do
      assert {:ok, "EXPLAIN SELECT 1"} = ClickHouse.apply_limit("EXPLAIN SELECT 1", 10)
    end

    test "rejects a single mutating non-query statement" do
      assert {:error, "Expected one ClickHouse query"} =
               ClickHouse.apply_limit("OPTIMIZE TABLE foo FINAL", 10)
    end

    test "moves a format clause outside the wrapping limit" do
      query = "SELECT number FROM numbers(100) LIMIT -50 FORMAT JSONEachRow"

      assert {:ok,
              "SELECT * FROM (SELECT number FROM numbers(100) LIMIT -50) LIMIT 10 FORMAT JSONEachRow"} =
               ClickHouse.apply_limit(query, 10)
    end

    test "places the limit before query-level settings" do
      assert {:ok, "SELECT number FROM numbers(100) LIMIT 10 SETTINGS max_threads = 1"} =
               ClickHouse.apply_limit(
                 "SELECT number FROM numbers(100) SETTINGS max_threads = 1",
                 10
               )
    end

    test "rejects multiple statements" do
      assert {:error, "Expected one ClickHouse query"} =
               ClickHouse.apply_limit("SELECT 1; SELECT 2", 10)
    end
  end

  describe "build_transformation_data/2" do
    test "passes through base data unchanged" do
      user = insert(:user)

      base_data = %{
        sources: [],
        dialect: "clickhouse",
        ast: [],
        sandboxed_query: nil
      }

      result = ClickHouse.build_transformation_data(user, base_data)

      assert result == base_data
    end
  end
end
