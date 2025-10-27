defmodule Logflare.Sql.DialectTranslationTest do
  use ExUnit.Case, async: true

  alias Logflare.Sql.DialectTranslation

  doctest DialectTranslation

  test "translates BigQuery backticks to PostgreSQL double quotes" do
    bq_query = "SELECT `field_name` FROM `table_name`"

    assert {:ok, pg_query} = DialectTranslation.translate_bq_to_pg(bq_query)
    assert is_binary(pg_query)

    # Should convert backticks to double quotes
    refute String.contains?(pg_query, "`")
    assert String.contains?(pg_query, "\"")
  end

  test "translates BigQuery field access to PostgreSQL JSON operators" do
    bq_query = "SELECT user_id FROM logs"

    assert {:ok, pg_query} = DialectTranslation.translate_bq_to_pg(bq_query)
    assert is_binary(pg_query)

    # Should convert field access to JSON operators
    assert String.contains?(pg_query, "body")
    assert String.contains?(pg_query, "->")
  end

  test "removes parentheses from CURRENT_TIMESTAMP function" do
    bq_query = "SELECT CURRENT_TIMESTAMP() as now"

    assert {:ok, pg_query} = DialectTranslation.translate_bq_to_pg(bq_query)
    assert is_binary(pg_query)

    # Should remove parentheses from CURRENT_TIMESTAMP
    refute String.contains?(pg_query, "CURRENT_TIMESTAMP()")
    assert String.contains?(pg_query, "current_timestamp")
  end

  test "handles null values in AST without syntax errors" do
    bq_query = """
    WITH cte1 AS (
      SELECT field1, field2 FROM table1 WHERE condition = 'value'
    ),
    cte2 AS (
      SELECT field3, field4 FROM table2 WHERE other = 'test'
    )
    SELECT * FROM cte1
    UNION ALL
    SELECT * FROM cte2
    """

    assert {:ok, pg_query} = DialectTranslation.translate_bq_to_pg(bq_query, "_analytics")
    assert is_binary(pg_query)

    # Should not contain syntax errors
    refute String.contains?(pg_query, "AS null")
    refute String.contains?(pg_query, "AS <null>")

    # Should have valid AS clauses
    assert String.match?(pg_query, ~r/AS \w+/)
  end

  test "handles multiple nested CTEs with metadata field access" do
    bq_query = """
    WITH edge_logs AS (
      SELECT timestamp, id, event_message, metadata FROM log_events
      WHERE project = 'test' AND timestamp > TIMESTAMP('2025-01-01')
    ),
    postgres_logs AS (
      SELECT timestamp, id, event_message FROM log_events
      WHERE project = 'test2'
    )
    SELECT * FROM edge_logs
    UNION ALL
    SELECT * FROM postgres_logs
    """

    assert {:ok, pg_query} = DialectTranslation.translate_bq_to_pg(bq_query, "_analytics")
    assert is_binary(pg_query)

    # Ensure no null values leaked into aliases
    refute String.contains?(String.downcase(pg_query), "as null")

    # Check for valid PG syntax
    # JSON ops
    assert String.contains?(pg_query, "->")
    # CTE preserved
    assert String.contains?(pg_query, "WITH")
  end

  test "translates REGEXP_CONTAINS to use text extraction for JSONB fields" do
    # This tests the fix for "operator does not exist: jsonb ~ unknown" error
    bq_query = """
    SELECT user_id, email
    FROM users
    WHERE REGEXP_CONTAINS(email, '@example\\.com$')
    """

    assert {:ok, pg_query} = DialectTranslation.translate_bq_to_pg(bq_query, "_analytics")
    assert is_binary(pg_query)

    # Should use ->> (text extraction) not -> (jsonb extraction) for regex
    # The regex operator ~ requires text operands
    assert String.contains?(pg_query, "->>") or String.contains?(pg_query, "::text")

    # Should not have the problematic "jsonb ~ unknown" pattern
    refute String.match?(pg_query, ~r/\-\>\s*'[^']+'\s*~/)

    # Should have the regex operator
    assert String.contains?(pg_query, "~")
  end

  test "handles REGEXP_CONTAINS in WHERE clause with other conditions" do
    bq_query = """
    SELECT id, email
    FROM logs
    WHERE status = 'active' AND REGEXP_CONTAINS(email, '@example\\.com$')
    """

    assert {:ok, pg_query} = DialectTranslation.translate_bq_to_pg(bq_query, "_analytics")
    assert is_binary(pg_query)

    # Should generate valid PG SQL with proper type casting
    assert String.contains?(pg_query, "~")

    # Should use text extraction operators
    assert String.contains?(pg_query, "->>")

    # Should have both conditions
    assert String.contains?(pg_query, "AND")
  end

  test "handles timestamp conversions from null identifiers" do
    # This tests for errors related to a CompoundIdentifier with null values
    bq_query = """
    SELECT
      id,
      timestamp,
      event_message,
      metadata
    FROM auth_logs
    WHERE timestamp > TIMESTAMP('2025-01-01')
    """

    assert {:ok, pg_query} = DialectTranslation.translate_bq_to_pg(bq_query, "_analytics")
    assert is_binary(pg_query)

    # Should have valid timestamp conversion
    assert String.contains?(pg_query, "to_timestamp")
    assert String.contains?(pg_query, "AT TIME ZONE")

    # Should not have malformed identifiers
    refute String.contains?(pg_query, "null.")
    refute String.contains?(pg_query, ".null")
  end
end
