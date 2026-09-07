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

    test "wraps OFFSET ROWS before adding a global limit" do
      for row_keyword <- ["ROW", "ROWS"] do
        query = "SELECT number FROM numbers(10) ORDER BY number OFFSET 2 #{row_keyword}"

        assert ClickHouse.apply_limit(query, 3) ==
                 {:ok, "SELECT * FROM (#{query}) LIMIT 3"}
      end
    end

    test "caps a larger existing limit" do
      assert {:ok, "SELECT number FROM numbers(100) LIMIT least(50, 10)"} =
               ClickHouse.apply_limit("SELECT number FROM numbers(100) LIMIT 50", 10)
    end

    test "wraps set operations before adding a global limit" do
      for query <- [
            "SELECT 1 UNION ALL SELECT 2",
            "SELECT 1 UNION DISTINCT SELECT 2",
            "SELECT 1 INTERSECT SELECT 2",
            "SELECT 1 EXCEPT SELECT 2"
          ] do
        expected = "SELECT * FROM (#{query}) LIMIT 3"

        assert {:ok, ^expected} = ClickHouse.apply_limit(query, 3)
      end
    end

    test "wraps a parenthesized set operation before adding a global limit" do
      query = "(SELECT 1 UNION ALL SELECT 2)"
      expected = "SELECT * FROM (SELECT 1 UNION ALL SELECT 2) LIMIT 3"

      assert {:ok, ^expected} = ClickHouse.apply_limit(query, 3)
    end

    test "wraps a parenthesized plain query before adding a global limit" do
      query = "(SELECT number FROM numbers(10))"
      expected = "SELECT * FROM ((SELECT number FROM numbers(10))) LIMIT 3"

      assert {:ok, ^expected} = ClickHouse.apply_limit(query, 3)
    end

    test "preserves ordering inside a parenthesized set operation" do
      query = "(SELECT 1 AS number UNION ALL SELECT 2 AS number ORDER BY number DESC)"

      expected =
        "SELECT * FROM (SELECT 1 AS number UNION ALL SELECT 2 AS number ORDER BY number DESC) LIMIT 1"

      assert {:ok, ^expected} = ClickHouse.apply_limit(query, 1)
    end

    test "moves terminal modifiers outside parenthesized set operations" do
      for terminal <- ["SETTINGS max_threads = 1", "FORMAT JSONEachRow"] do
        query =
          "(SELECT 1 AS number UNION ALL SELECT 2 AS number ORDER BY number DESC) #{terminal}"

        expected =
          "SELECT * FROM (SELECT 1 AS number UNION ALL SELECT 2 AS number ORDER BY number DESC) LIMIT 1 #{terminal}"

        assert {:ok, ^expected} = ClickHouse.apply_limit(query, 1)
      end
    end

    test "combines nested and outer SETTINGS while preserving branch modifiers" do
      query =
        "(SELECT 1 AS n UNION ALL SELECT 2 AS n ORDER BY n SETTINGS max_threads = 1) SETTINGS optimize_read_in_order = 0"

      expected =
        "SELECT * FROM (SELECT 1 AS n UNION ALL SELECT 2 AS n ORDER BY n) LIMIT 1 SETTINGS max_threads = 1, optimize_read_in_order = 0"

      assert {:ok, ^expected} = ClickHouse.apply_limit(query, 1)
    end

    test "keeps FETCH and ordering in the final set-operation branch" do
      cases = [
        {"SELECT 1 AS n UNION ALL SELECT 2 AS n ORDER BY n FETCH FIRST 1 ROWS ONLY", 2},
        {"SELECT 1 AS n UNION ALL SELECT 1 AS n UNION ALL SELECT 2 AS n ORDER BY n FETCH FIRST 1 ROWS WITH TIES",
         1}
      ]

      for {query, max_rows} <- cases do
        assert ClickHouse.apply_limit(query, max_rows) ==
                 {:ok, "SELECT * FROM (#{query}) LIMIT #{max_rows}"}
      end
    end

    test "preserves final-branch ordering and limits under the result cap" do
      query =
        "SELECT 100 AS n UNION ALL SELECT number AS n FROM numbers(2) ORDER BY n LIMIT 1"

      assert ClickHouse.apply_limit(query, 10) == {:ok, "SELECT * FROM (#{query}) LIMIT 10"}
    end

    test "preserves a set-operation branch limit and offset" do
      query =
        "SELECT number FROM numbers(5) UNION ALL SELECT number + 5 FROM numbers(5) ORDER BY number LIMIT 4 OFFSET 2"

      assert ClickHouse.apply_limit(query, 3) == {:ok, "SELECT * FROM (#{query}) LIMIT 3"}
    end

    test "preserves LIMIT BY in the final set-operation branch" do
      query =
        "SELECT 1 AS n UNION ALL SELECT 2 AS n UNION ALL SELECT 3 AS n UNION ALL SELECT 4 AS n LIMIT 1 BY n"

      assert ClickHouse.apply_limit(query, 3) == {:ok, "SELECT * FROM (#{query}) LIMIT 3"}
    end

    test "keeps a branch alias used by LIMIT in scope" do
      query =
        "SELECT number AS n FROM numbers(5) UNION ALL SELECT 3 AS cap FROM numbers(5) LIMIT cap"

      assert ClickHouse.apply_limit(query, 2) == {:ok, "SELECT * FROM (#{query}) LIMIT 2"}
    end

    test "keeps set-operation expressions that depend on a CTE in scope" do
      queries = [
        "WITH cap AS (SELECT 3 AS n) SELECT number FROM numbers(5) UNION ALL SELECT number FROM numbers(5) LIMIT (SELECT n FROM cap)",
        "WITH offset_rows AS (SELECT 2 AS n) SELECT number FROM numbers(5) UNION ALL SELECT number FROM numbers(5) LIMIT 3 OFFSET (SELECT n FROM offset_rows)",
        "WITH groups AS (SELECT 1 AS g) SELECT number FROM numbers(5) UNION ALL SELECT number FROM numbers(5) LIMIT 1 BY (SELECT g FROM groups)"
      ]

      for query <- queries do
        assert ClickHouse.apply_limit(query, 2) == {:ok, "SELECT * FROM (#{query}) LIMIT 2"}
      end
    end

    test "keeps WITH FILL bounds inside the final set-operation branch" do
      queries = [
        "WITH bounds AS (SELECT 5 AS max) SELECT 0 AS n UNION ALL SELECT 2 AS n ORDER BY n WITH FILL TO assumeNotNull((SELECT max FROM bounds)) STEP 1",
        "SELECT 0 AS n UNION ALL SELECT 2 AS n ORDER BY n WITH FILL FROM 0 TO 5 STEP 1"
      ]

      for query <- queries do
        assert ClickHouse.apply_limit(query, 2) == {:ok, "SELECT * FROM (#{query}) LIMIT 2"}
      end
    end

    test "keeps set-operation SETTINGS outermost without moving branch limits" do
      query = "SELECT 1 AS n UNION ALL SELECT 2 AS n LIMIT 100 SETTINGS max_threads = 1"

      expected =
        "SELECT * FROM (SELECT 1 AS n UNION ALL SELECT 2 AS n LIMIT 100) LIMIT 3 SETTINGS max_threads = 1"

      assert {:ok, ^expected} = ClickHouse.apply_limit(query, 3)
    end

    test "keeps SETTINGS outside a complex set-operation limit" do
      query =
        "SELECT 1 AS n UNION ALL SELECT 2 AS n ORDER BY n LIMIT -1 SETTINGS max_threads = 1"

      expected =
        "SELECT * FROM (SELECT 1 AS n UNION ALL SELECT 2 AS n ORDER BY n LIMIT -1) LIMIT 3 SETTINGS max_threads = 1"

      assert {:ok, ^expected} = ClickHouse.apply_limit(query, 3)
    end

    test "preserves result modifiers for every set-operation projection shape" do
      queries = [
        "SELECT number AS value FROM numbers(5) UNION ALL SELECT number AS value FROM numbers(5) ORDER BY number DESC LIMIT 3",
        "SELECT number AS value FROM numbers(5) UNION ALL SELECT number AS value FROM numbers(5) ORDER BY value DESC LIMIT 3",
        "SELECT * FROM numbers(5) UNION ALL SELECT * FROM numbers(5) ORDER BY number DESC LIMIT 3",
        "SELECT * FROM numbers(5) UNION ALL SELECT number AS value FROM numbers(5) LIMIT 1 BY value"
      ]

      for query <- queries do
        assert ClickHouse.apply_limit(query, 2) == {:ok, "SELECT * FROM (#{query}) LIMIT 2"}
      end
    end

    test "preserves negative and fractional limits in the final branch" do
      for existing_limit <- ["-5", "0.5"] do
        query =
          "SELECT number FROM numbers(10) UNION ALL SELECT number + 10 FROM numbers(10) ORDER BY number LIMIT #{existing_limit}"

        assert ClickHouse.apply_limit(query, 3) == {:ok, "SELECT * FROM (#{query}) LIMIT 3"}
      end
    end

    test "keeps FORMAT outside a set operation with branch modifiers" do
      query =
        "SELECT number FROM numbers(10) UNION ALL SELECT number + 10 FROM numbers(10) ORDER BY number LIMIT -5 FORMAT JSONEachRow"

      expected =
        "SELECT * FROM (SELECT number FROM numbers(10) UNION ALL SELECT number + 10 FROM numbers(10) ORDER BY number LIMIT -5) LIMIT 3 FORMAT JSONEachRow"

      assert {:ok, ^expected} = ClickHouse.apply_limit(query, 3)
    end

    test "applies global ordering when the query has an explicit outer SELECT" do
      query =
        "SELECT * FROM (SELECT number FROM numbers(5) UNION ALL SELECT number + 5 FROM numbers(5)) ORDER BY number DESC LIMIT 4"

      expected =
        "SELECT * FROM (SELECT number FROM numbers(5) UNION ALL SELECT number + 5 FROM numbers(5)) ORDER BY number DESC LIMIT least(4, 3)"

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
