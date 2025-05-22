defmodule Logflare.Backends.Adaptor.ClickhouseAdaptor.QueryTemplatesTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor.ClickhouseAdaptor.QueryTemplates

  doctest QueryTemplates

  describe "create_log_ingest_table_statement/2" do
    test "Generates a valid statement when given a table name" do
      table_name = "foo"

      statement = QueryTemplates.create_log_ingest_table_statement(table_name)

      assert statement =~ "CREATE TABLE IF NOT EXISTS #{table_name}"
      assert statement =~ "TTL toDateTime(\"timestamp\") + INTERVAL 90 DAY"
    end

    test "Allows the TTL to be adjusted via opts" do
      table_name = "foo"

      statement = QueryTemplates.create_log_ingest_table_statement(table_name, ttl_days: 5)

      assert statement =~ "CREATE TABLE IF NOT EXISTS #{table_name}"
      assert statement =~ "TTL toDateTime(\"timestamp\") + INTERVAL 5 DAY"
    end

    test "Removes the TTL when `ttl_days` is set to nil" do
      table_name = "foo"

      statement = QueryTemplates.create_log_ingest_table_statement(table_name, ttl_days: nil)

      assert statement =~ "CREATE TABLE IF NOT EXISTS #{table_name}"
      refute statement =~ "TTL"
    end
  end
end
