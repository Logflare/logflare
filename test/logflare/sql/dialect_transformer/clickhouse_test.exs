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

    test "hoists ordering from a parenthesized set operation" do
      query = "(SELECT 1 AS number UNION ALL SELECT 2 AS number ORDER BY number DESC)"

      expected =
        "SELECT * FROM (SELECT 1 AS number UNION ALL SELECT 2 AS number) ORDER BY number DESC LIMIT 1"

      assert {:ok, ^expected} = ClickHouse.apply_limit(query, 1)
    end

    test "hoists modifiers through terminal-only parenthesis wrappers" do
      for terminal <- ["SETTINGS max_threads = 1", "FORMAT JSONEachRow"] do
        query =
          "(SELECT 1 AS number UNION ALL SELECT 2 AS number ORDER BY number DESC) #{terminal}"

        expected =
          "SELECT * FROM (SELECT 1 AS number UNION ALL SELECT 2 AS number) ORDER BY number DESC LIMIT 1 #{terminal}"

        assert {:ok, ^expected} = ClickHouse.apply_limit(query, 1)
      end
    end

    test "combines nested and outer SETTINGS while collapsing parentheses" do
      query =
        "(SELECT 1 AS n UNION ALL SELECT 2 AS n ORDER BY n SETTINGS max_threads = 1) SETTINGS optimize_read_in_order = 0"

      expected =
        "SELECT * FROM (SELECT 1 AS n UNION ALL SELECT 2 AS n) ORDER BY n LIMIT 1 SETTINGS max_threads = 1, optimize_read_in_order = 0"

      assert {:ok, ^expected} = ClickHouse.apply_limit(query, 1)
    end

    test "keeps FETCH with set-operation ordering under the endpoint cap" do
      cases = [
        {
          "SELECT 1 AS n UNION ALL SELECT 2 AS n ORDER BY n FETCH FIRST 1 ROWS ONLY",
          "SELECT * FROM (SELECT * FROM (SELECT 1 AS n UNION ALL SELECT 2 AS n) ORDER BY n FETCH FIRST 1 ROWS ONLY) LIMIT 2",
          2
        },
        {
          "SELECT 1 AS n UNION ALL SELECT 1 AS n UNION ALL SELECT 2 AS n ORDER BY n FETCH FIRST 1 ROWS WITH TIES",
          "SELECT * FROM (SELECT * FROM (SELECT 1 AS n UNION ALL SELECT 1 AS n UNION ALL SELECT 2 AS n) ORDER BY n FETCH FIRST 1 ROWS WITH TIES) LIMIT 1",
          1
        }
      ]

      for {query, expected, max_rows} <- cases do
        assert {:ok, ^expected} = ClickHouse.apply_limit(query, max_rows)
      end
    end

    test "hoists a set operation's ordering onto the global limit" do
      query =
        "SELECT number FROM numbers(5) UNION ALL SELECT number + 5 FROM numbers(5) ORDER BY number DESC"

      expected =
        "SELECT * FROM (SELECT number FROM numbers(5) UNION ALL SELECT number + 5 FROM numbers(5)) ORDER BY number DESC LIMIT 3"

      assert {:ok, ^expected} = ClickHouse.apply_limit(query, 3)
    end

    test "applies a set operation's existing limit and offset globally" do
      query =
        "SELECT number FROM numbers(5) UNION ALL SELECT number + 5 FROM numbers(5) ORDER BY number LIMIT 4 OFFSET 2"

      expected =
        "SELECT * FROM (SELECT number FROM numbers(5) UNION ALL SELECT number + 5 FROM numbers(5)) ORDER BY number LIMIT least(4, 3) OFFSET 2"

      assert {:ok, ^expected} = ClickHouse.apply_limit(query, 3)
    end

    test "keeps LIMIT BY together over a completed set operation" do
      query =
        "SELECT 1 AS n UNION ALL SELECT 2 AS n UNION ALL SELECT 3 AS n UNION ALL SELECT 4 AS n LIMIT 1 BY n"

      expected =
        "SELECT * FROM (SELECT * FROM (SELECT 1 AS n UNION ALL SELECT 2 AS n UNION ALL SELECT 3 AS n UNION ALL SELECT 4 AS n) LIMIT 1 BY n) LIMIT 3"

      assert {:ok, ^expected} = ClickHouse.apply_limit(query, 3)
    end

    test "preserves coupled modifiers when LIMIT BY references an unexposed column" do
      query =
        "SELECT number AS value FROM numbers(5) UNION ALL SELECT number AS value FROM numbers(5) ORDER BY value DESC LIMIT 1 BY number"

      expected = "SELECT * FROM (#{query}) LIMIT 2"

      assert {:ok, ^expected} = ClickHouse.apply_limit(query, 2)
    end

    test "keeps a CTE-dependent set-operation limit in scope" do
      query =
        "WITH cap AS (SELECT 3 AS n) SELECT number FROM numbers(5) UNION ALL SELECT number FROM numbers(5) LIMIT (SELECT n FROM cap)"

      expected = "SELECT * FROM (#{query}) LIMIT 2"

      assert {:ok, ^expected} = ClickHouse.apply_limit(query, 2)
    end

    test "keeps a CTE-dependent set-operation offset in scope" do
      query =
        "WITH offset_rows AS (SELECT 2 AS n) SELECT number FROM numbers(5) UNION ALL SELECT number FROM numbers(5) LIMIT 3 OFFSET (SELECT n FROM offset_rows)"

      expected = "SELECT * FROM (#{query}) LIMIT 2"

      assert {:ok, ^expected} = ClickHouse.apply_limit(query, 2)
    end

    test "keeps a CTE-dependent set-operation LIMIT BY in scope" do
      query =
        "WITH groups AS (SELECT 1 AS g) SELECT number FROM numbers(5) UNION ALL SELECT number FROM numbers(5) LIMIT 1 BY (SELECT g FROM groups)"

      expected = "SELECT * FROM (#{query}) LIMIT 2"

      assert {:ok, ^expected} = ClickHouse.apply_limit(query, 2)
    end

    test "keeps scoped WITH FILL bounds inside the set operation" do
      query =
        "WITH bounds AS (SELECT 5 AS max) SELECT 0 AS n UNION ALL SELECT 2 AS n ORDER BY n WITH FILL TO assumeNotNull((SELECT max FROM bounds)) STEP 1"

      expected = "SELECT * FROM (#{query}) LIMIT 2"

      assert {:ok, ^expected} = ClickHouse.apply_limit(query, 2)
    end

    test "hoists constant WITH FILL bounds with set-operation ordering" do
      query =
        "SELECT 0 AS n UNION ALL SELECT 2 AS n ORDER BY n WITH FILL FROM 0 TO 5 STEP 1"

      expected =
        "SELECT * FROM (SELECT 0 AS n UNION ALL SELECT 2 AS n) ORDER BY n WITH FILL FROM 0 TO 5 STEP 1 LIMIT 2"

      assert {:ok, ^expected} = ClickHouse.apply_limit(query, 2)
    end

    test "keeps set-operation SETTINGS outermost" do
      query =
        "SELECT 1 AS n UNION ALL SELECT 2 AS n LIMIT 100 SETTINGS max_threads = 1"

      expected =
        "SELECT * FROM (SELECT 1 AS n UNION ALL SELECT 2 AS n) LIMIT least(100, 3) SETTINGS max_threads = 1"

      assert {:ok, ^expected} = ClickHouse.apply_limit(query, 3)
    end

    test "keeps SETTINGS outside a complex set-operation limit" do
      query =
        "SELECT 1 AS n UNION ALL SELECT 2 AS n ORDER BY n LIMIT -1 SETTINGS max_threads = 1"

      expected =
        "SELECT * FROM (SELECT * FROM (SELECT 1 AS n UNION ALL SELECT 2 AS n) ORDER BY n LIMIT -1) LIMIT 3 SETTINGS max_threads = 1"

      assert {:ok, ^expected} = ClickHouse.apply_limit(query, 3)
    end

    test "preserves coupled modifiers when ordering is not exposed" do
      query =
        "SELECT number AS value FROM numbers(5) UNION ALL SELECT number AS value FROM numbers(5) ORDER BY number DESC LIMIT 3"

      expected =
        "SELECT * FROM (SELECT number AS value FROM numbers(5) UNION ALL SELECT number AS value FROM numbers(5) ORDER BY number DESC LIMIT 3) LIMIT 2"

      assert {:ok, ^expected} = ClickHouse.apply_limit(query, 2)
    end

    test "hoists ordering by an exposed set-operation alias" do
      query =
        "SELECT number AS value FROM numbers(5) UNION ALL SELECT number AS value FROM numbers(5) ORDER BY value DESC LIMIT 3"

      expected =
        "SELECT * FROM (SELECT number AS value FROM numbers(5) UNION ALL SELECT number AS value FROM numbers(5)) ORDER BY value DESC LIMIT least(3, 2)"

      assert {:ok, ^expected} = ClickHouse.apply_limit(query, 2)
    end

    test "keeps wildcard ordering in its original scope" do
      query =
        "SELECT * FROM numbers(5) UNION ALL SELECT * FROM numbers(5) ORDER BY number DESC LIMIT 3"

      expected = "SELECT * FROM (#{query}) LIMIT 2"

      assert {:ok, ^expected} = ClickHouse.apply_limit(query, 2)
    end

    test "preserves wildcard modifiers that reference a right-branch alias" do
      base_query =
        "SELECT * FROM numbers(5) UNION ALL SELECT number AS value FROM numbers(5)"

      for modifier <- ["ORDER BY value DESC LIMIT 3", "LIMIT 1 BY value"] do
        query = "#{base_query} #{modifier}"
        expected = "SELECT * FROM (#{query}) LIMIT 2"

        assert {:ok, ^expected} = ClickHouse.apply_limit(query, 2)
      end
    end

    test "caps negative and fractional set-operation limits after global evaluation" do
      for existing_limit <- ["-5", "0.5"] do
        query =
          "SELECT number FROM numbers(10) UNION ALL SELECT number + 10 FROM numbers(10) ORDER BY number LIMIT #{existing_limit}"

        expected =
          "SELECT * FROM (SELECT * FROM (SELECT number FROM numbers(10) UNION ALL SELECT number + 10 FROM numbers(10)) ORDER BY number LIMIT #{existing_limit}) LIMIT 3"

        assert {:ok, ^expected} = ClickHouse.apply_limit(query, 3)
      end
    end

    test "keeps FORMAT outside nested set-operation limits" do
      query =
        "SELECT number FROM numbers(10) UNION ALL SELECT number + 10 FROM numbers(10) ORDER BY number LIMIT -5 FORMAT JSONEachRow"

      expected =
        "SELECT * FROM (SELECT * FROM (SELECT number FROM numbers(10) UNION ALL SELECT number + 10 FROM numbers(10)) ORDER BY number LIMIT -5) LIMIT 3 FORMAT JSONEachRow"

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
