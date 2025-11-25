defmodule Logflare.Sql.DialectTransformerTest do
  use ExUnit.Case, async: true

  alias Logflare.Sql.DialectTransformer
  doctest DialectTransformer

  describe "for_dialect/1" do
    test "returns BigQuery module for `:bq_sql` atom" do
      assert DialectTransformer.for_dialect(:bq_sql) == DialectTransformer.BigQuery
    end

    test "returns BigQuery module for `bigquery` string" do
      assert DialectTransformer.for_dialect("bigquery") == DialectTransformer.BigQuery
    end

    test "returns ClickHouse module for `:ch_sql` atom" do
      assert DialectTransformer.for_dialect(:ch_sql) == DialectTransformer.ClickHouse
    end

    test "returns ClickHouse module for `clickhouse` string" do
      assert DialectTransformer.for_dialect("clickhouse") == DialectTransformer.ClickHouse
    end

    test "returns Postgres module for `:pg_sql` atom" do
      assert DialectTransformer.for_dialect(:pg_sql) == DialectTransformer.Postgres
    end

    test "returns Postgres module for `postgres` string" do
      assert DialectTransformer.for_dialect("postgres") == DialectTransformer.Postgres
    end
  end

  describe "to_dialect/1" do
    test "converts `:bq_sql` to `bigquery` string" do
      assert DialectTransformer.to_dialect(:bq_sql) == "bigquery"
    end

    test "converts `:ch_sql` to `clickhouse` string" do
      assert DialectTransformer.to_dialect(:ch_sql) == "clickhouse"
    end

    test "converts `:pg_sql` to `postgres` string" do
      assert DialectTransformer.to_dialect(:pg_sql) == "postgres"
    end
  end
end
