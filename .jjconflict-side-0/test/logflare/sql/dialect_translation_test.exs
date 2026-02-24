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

  test "handles CTE with UNNEST metadata alias correctly" do
    bq_query = """
    WITH auth_logs AS (
      SELECT t.timestamp, t.id, t.event_message, t.metadata
      FROM `project.dataset.table` AS t
      CROSS JOIN UNNEST(metadata) AS m
      WHERE t.project = 'test'
    )
    SELECT
      id,
      auth_logs.timestamp,
      event_message,
      metadata.level,
      metadata.status,
      metadata.msg
    FROM auth_logs
    CROSS JOIN UNNEST(metadata) AS metadata
    ORDER BY timestamp DESC
    LIMIT 100
    """

    assert {:ok, pg_query} = DialectTranslation.translate_bq_to_pg(bq_query)
    assert is_binary(pg_query)

    # Should use metadata field for accessing nested properties, not timestamp
    assert String.contains?(pg_query, "metadata")
    refute String.contains?(pg_query, "timestamp #>>")
    refute String.contains?(pg_query, "to_timestamp") and String.contains?(pg_query, "#>>")

    # Should have proper JSONB access for metadata.level (direct access, not doubled path)
    assert String.contains?(pg_query, "metadata -> 'level'") or
             String.contains?(pg_query, "metadata ->> 'level'")
  end

  test "translates three-part CTE column access to JSON operators" do
    bq_query = """
    WITH logs AS (
      SELECT t.metadata FROM `table` AS t
    )
    SELECT logs.metadata.msg, logs.metadata.level FROM logs
    """

    assert {:ok, pg_query} = DialectTranslation.translate_bq_to_pg(bq_query)
    assert is_binary(pg_query)

    refute String.match?(pg_query, ~r/logs\.metadata\.msg(?!\s*->)/)
    assert String.contains?(pg_query, "logs.metadata ->")
    assert String.contains?(pg_query, "'msg'")
    assert String.contains?(pg_query, "'level'")
  end

  test "translates deeply nested CTE column access to JSON path operators" do
    bq_query = """
    WITH logs AS (
      SELECT t.metadata FROM `table` AS t
    )
    SELECT logs.metadata.nested.deep FROM logs
    """

    assert {:ok, pg_query} = DialectTranslation.translate_bq_to_pg(bq_query)
    assert is_binary(pg_query)

    assert String.contains?(pg_query, "logs.metadata")
    assert String.contains?(pg_query, "#>") or String.contains?(pg_query, "#>>")
    assert String.contains?(pg_query, "{nested,deep}")
  end

  test "handles missing UNNEST alias gracefully without crashing" do
    bq_query = """
    WITH logs AS (
      SELECT t.metadata
      FROM `table` AS t
      CROSS JOIN UNNEST(t.metadata) AS m
    )
    SELECT m.field FROM logs
    """

    assert {:ok, pg_query} = DialectTranslation.translate_bq_to_pg(bq_query)
    assert is_binary(pg_query)
    assert String.contains?(pg_query, "->")
  end

  test "handles UNNEST alias matching CTE column name without doubling path" do
    bq_query = """
    WITH logs AS (
      SELECT t.timestamp, t.id, t.event_message, t.metadata
      FROM `table` AS t
      CROSS JOIN UNNEST(t.metadata) AS m
      WHERE t.project = @project
    )
    SELECT id, logs.timestamp, event_message,
           metadata.level, metadata.status, metadata.msg as msg
    FROM logs
    CROSS JOIN UNNEST(metadata) AS metadata
    ORDER BY timestamp DESC
    LIMIT 100
    """

    assert {:ok, pg_query} = DialectTranslation.translate_bq_to_pg(bq_query, "_analytics")
    assert is_binary(pg_query)

    refute String.contains?(pg_query, "{metadata,level}")
    refute String.contains?(pg_query, "{metadata,status}")
    refute String.contains?(pg_query, "{metadata,msg}")

    assert String.contains?(pg_query, "metadata -> 'level'") or
             String.contains?(pg_query, "metadata ->> 'level'")

    assert String.contains?(pg_query, "metadata -> 'msg'") or
             String.contains?(pg_query, "metadata ->> 'msg'")
  end

  test "handles CROSS JOIN UNNEST being dropped from AST" do
    bq_query = """
    WITH data AS (
      SELECT t.timestamp, t.metadata
      FROM `table` AS t
      CROSS JOIN UNNEST(t.metadata) AS m
      WHERE m.project = 'test'
    )
    SELECT * FROM data
    """

    assert {:ok, pg_query} = DialectTranslation.translate_bq_to_pg(bq_query)
    assert is_binary(pg_query)

    # CROSS JOIN UNNEST should be removed
    refute String.contains?(String.upcase(pg_query), "CROSS JOIN")
    refute String.contains?(String.upcase(pg_query), "UNNEST")

    # JSONB path operators should be present
    assert String.contains?(pg_query, "#>>") or String.contains?(pg_query, "->>")
  end

  test "handles null CompoundIdentifier values" do
    bq_query = """
    SELECT t.field1, t.field2
    FROM `table` AS t
    CROSS JOIN UNNEST(t.metadata) AS m
    WHERE m.nested_field = 'value'
    """

    assert {:ok, pg_query} = DialectTranslation.translate_bq_to_pg(bq_query)
    assert is_binary(pg_query)

    # Should produce valid PostgreSQL
    assert String.contains?(pg_query, "body")
    assert String.contains?(pg_query, "->") or String.contains?(pg_query, "->>")
  end

  test "casts CTE identifiers to text when used with regex operators" do
    # This tests the fix for "operator does not exist: jsonb ~ unknown" when
    # a CTE field (which is JSONB) is used in a regex comparison
    bq_query = """
    WITH postgres_logs AS (
      SELECT t.timestamp, t.event_message
      FROM `table` AS t
      WHERE t.project = 'test'
    )
    SELECT event_message
    FROM postgres_logs
    WHERE REGEXP_CONTAINS(event_message, 'cron job')
    """

    assert {:ok, pg_query} = DialectTranslation.translate_bq_to_pg(bq_query)
    assert is_binary(pg_query)

    # Should cast event_message to text for regex comparison
    # Look for either ::text or ->> (text extraction)
    assert String.contains?(pg_query, "::text") or String.contains?(pg_query, "::TEXT")

    # Should have the regex operator
    assert String.contains?(pg_query, "~")
  end

  test "handles CAST to numeric types on CTE fields by adding ::TEXT first" do
    # This tests the fix for "cannot cast type jsonb to bigint" errors
    # when CTE fields (which are JSONB) are cast to numeric types
    bq_query = """
    WITH logs AS (
      SELECT t.timestamp, t.id
      FROM `table` AS t
      WHERE t.project = 'test'
    )
    SELECT *
    FROM logs
    WHERE CAST(timestamp AS BIGINT) > 1000000
    """

    assert {:ok, pg_query} = DialectTranslation.translate_bq_to_pg(bq_query)
    assert is_binary(pg_query)

    # Should add ::TEXT before casting to BIGINT
    # Pattern: CAST(timestamp::TEXT AS BIGINT) or CAST(CAST(timestamp AS TEXT) AS BIGINT)
    assert String.contains?(pg_query, "::text") or String.contains?(pg_query, "::TEXT") or
             String.contains?(pg_query, "AS TEXT")
  end

  test "converts SAFE_CAST and INT64 from BigQuery to PostgreSQL" do
    # This tests conversion of BigQuery SAFE_CAST to PostgreSQL CAST
    # and INT64 type to BIGINT
    bq_query = """
    SELECT SAFE_CAST(status AS INT64) as status_code
    FROM logs
    WHERE SAFE_CAST(code AS INT64) > 400
    """

    assert {:ok, pg_query} = DialectTranslation.translate_bq_to_pg(bq_query)
    assert is_binary(pg_query)

    # Should not contain SAFE_CAST or INT64
    refute String.contains?(pg_query, "SAFE_CAST")
    refute String.contains?(pg_query, "INT64")

    # Should contain regular CAST and BIGINT
    assert String.contains?(pg_query, "CAST")
    assert String.contains?(pg_query, "BIGINT") or String.contains?(pg_query, "bigint")
  end
end
