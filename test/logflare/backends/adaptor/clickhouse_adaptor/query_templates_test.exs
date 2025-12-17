defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.QueryTemplatesTest do
  use Logflare.DataCase, async: true

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.QueryTemplates

  doctest QueryTemplates

  describe "grant_check_statement/2" do
    test "Generates the default grant check statement when provided with no arguments" do
      assert QueryTemplates.grant_check_statement() ==
               "CHECK GRANT CREATE TABLE, ALTER TABLE, INSERT, SELECT, DROP TABLE, CREATE VIEW, DROP VIEW ON *"
    end

    test "Will produce a more verbose grant check statement when the `database` option is provided" do
      assert QueryTemplates.grant_check_statement(database: "foo") ==
               "CHECK GRANT CREATE TABLE, ALTER TABLE, INSERT, SELECT, DROP TABLE, CREATE VIEW, DROP VIEW ON foo.*"
    end
  end

  describe "create_ingest_table_statement/2" do
    test "Generates a valid statement when given a table name" do
      table_name = "foo"
      statement = QueryTemplates.create_ingest_table_statement(table_name)

      assert statement =~ "CREATE TABLE IF NOT EXISTS #{table_name}"
      assert statement =~ "TTL toDateTime(timestamp) + INTERVAL 5 DAY"
    end

    test "prefixes the database name to the table name" do
      database = "bar"
      table_name = "foo"
      statement = QueryTemplates.create_ingest_table_statement(table_name, database: database)

      assert statement =~ "CREATE TABLE IF NOT EXISTS #{database}.#{table_name}"
      assert statement =~ "TTL toDateTime(timestamp) + INTERVAL 5 DAY"
    end

    test "Defaults to using the `MergeTree` engine" do
      statement = QueryTemplates.create_ingest_table_statement("foo")

      assert statement =~ "ENGINE = MergeTree"
    end

    test "Allows the engine to be adjusted via opts" do
      table_name = "foo"
      custom_engine = "ReplacingMergeTree"

      statement =
        QueryTemplates.create_ingest_table_statement(table_name, engine: custom_engine)

      assert statement =~ "ENGINE = #{custom_engine}"
    end

    test "Allows the TTL to be adjusted via opts" do
      table_name = "foo"
      statement = QueryTemplates.create_ingest_table_statement(table_name, ttl_days: 10)

      assert statement =~ "CREATE TABLE IF NOT EXISTS #{table_name}"
      assert statement =~ "TTL toDateTime(timestamp) + INTERVAL 10 DAY"
    end

    test "Removes the TTL when `ttl_days` is set to nil" do
      table_name = "foo"
      statement = QueryTemplates.create_ingest_table_statement(table_name, ttl_days: nil)

      assert statement =~ "CREATE TABLE IF NOT EXISTS #{table_name}"
      refute statement =~ "TTL"
    end

    test "Removes the TTL when `ttl_days` is set to something other than a positive integer" do
      table_name = "foo"
      statement = QueryTemplates.create_ingest_table_statement(table_name, ttl_days: "pizza")

      assert statement =~ "CREATE TABLE IF NOT EXISTS #{table_name}"
      refute statement =~ "TTL"
    end
  end
end
