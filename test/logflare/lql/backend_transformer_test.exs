defmodule Logflare.Lql.BackendTransformerTest do
  use ExUnit.Case, async: true

  alias Logflare.Lql.BackendTransformer
  alias Logflare.Lql.BackendTransformer.BigQuery
  alias Logflare.Lql.BackendTransformer.ClickHouse
  alias Logflare.Lql.BackendTransformer.Postgres

  describe "for_dialect/1" do
    test "returns BigQuery transformer for 'bigquery' string" do
      assert BackendTransformer.for_dialect("bigquery") == BigQuery
    end

    test "returns BigQuery transformer for :bigquery atom" do
      assert BackendTransformer.for_dialect(:bigquery) == BigQuery
    end

    test "returns ClickHouse transformer for 'clickhouse' string" do
      assert BackendTransformer.for_dialect("clickhouse") == ClickHouse
    end

    test "returns ClickHouse transformer for :clickhouse atom" do
      assert BackendTransformer.for_dialect(:clickhouse) == ClickHouse
    end

    test "returns Postgres transformer for 'postgres' string" do
      assert BackendTransformer.for_dialect("postgres") == Postgres
    end

    test "returns Postgres transformer for :postgres atom" do
      assert BackendTransformer.for_dialect(:postgres) == Postgres
    end

    test "raises FunctionClauseError for unknown dialect" do
      assert_raise FunctionClauseError, fn ->
        BackendTransformer.for_dialect("unknown")
      end
    end
  end

  describe "to_dialect/1" do
    test "returns bigquery dialect string when provided with the proper atom" do
      assert BackendTransformer.to_dialect(:bigquery) == "bigquery"
    end

    test "returns clickhouse dialect string when provided with the proper atom" do
      assert BackendTransformer.to_dialect(:clickhouse) == "clickhouse"
    end

    test "returns postgres dialect string when provided with the proper atom" do
      assert BackendTransformer.to_dialect(:postgres) == "postgres"
    end
  end
end
