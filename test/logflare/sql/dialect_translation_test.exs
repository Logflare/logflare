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
end
