defmodule Logflare.Backends.Adaptor.ClickhouseAdaptor.QueryTemplatesTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor.ClickhouseAdaptor.QueryTemplates

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

  describe "create_log_ingest_table_statement/2" do
    test "Generates a valid statement when given a table name" do
      table_name = "foo"
      statement = QueryTemplates.create_log_ingest_table_statement(table_name)

      assert statement =~ "CREATE TABLE IF NOT EXISTS #{table_name}"
      assert statement =~ "TTL toDateTime(timestamp) + INTERVAL 3 DAY"
    end

    test "Allows the TTL to be adjusted via opts" do
      table_name = "foo"
      statement = QueryTemplates.create_log_ingest_table_statement(table_name, ttl_days: 5)

      assert statement =~ "CREATE TABLE IF NOT EXISTS #{table_name}"
      assert statement =~ "TTL toDateTime(timestamp) + INTERVAL 5 DAY"
    end

    test "Removes the TTL when `ttl_days` is set to nil" do
      table_name = "foo"
      statement = QueryTemplates.create_log_ingest_table_statement(table_name, ttl_days: nil)

      assert statement =~ "CREATE TABLE IF NOT EXISTS #{table_name}"
      refute statement =~ "TTL"
    end

    test "Removes the TTL when `ttl_days` is set to something other than a positive integer" do
      table_name = "foo"
      statement = QueryTemplates.create_log_ingest_table_statement(table_name, ttl_days: "pizza")

      assert statement =~ "CREATE TABLE IF NOT EXISTS #{table_name}"
      refute statement =~ "TTL"
    end
  end

  describe "create_key_type_counts_table_statement/1" do
    test "Generates a valid statement when provided with no options" do
      statement = QueryTemplates.create_key_type_counts_table_statement()

      assert statement =~ "CREATE TABLE IF NOT EXISTS key_type_counts_per_minute"
    end

    test "Will produce a more verbose statement when the `database` option is provided" do
      statement = QueryTemplates.create_key_type_counts_table_statement(database: "foo")

      assert statement =~ "CREATE TABLE IF NOT EXISTS foo.key_type_counts_per_minute"
    end

    test "Allows the default table name to be changed when providing the `table` option" do
      statement = QueryTemplates.create_key_type_counts_table_statement(table: "bla")

      assert statement =~ "CREATE TABLE IF NOT EXISTS bla"
    end

    test "Supports a verbose `<database>.<table>` statement when providing options" do
      statement =
        QueryTemplates.create_key_type_counts_table_statement(database: "foo", table: "bar")

      assert statement =~ "CREATE TABLE IF NOT EXISTS foo.bar"
    end
  end

  describe "create_materialized_view_statement/1" do
    test "Generates a valid statement when provided with a source table name" do
      statement = QueryTemplates.create_materialized_view_statement("source_table_123")

      assert statement =~
               "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_key_type_counts_per_minute TO key_type_counts_per_minute"

      assert statement =~ "FROM source_table_123"
    end

    test "Will produce a more verbose statement when the `database` option is provided" do
      statement =
        QueryTemplates.create_materialized_view_statement("source_table_123", database: "bla")

      assert statement =~
               "CREATE MATERIALIZED VIEW IF NOT EXISTS bla.mv_key_type_counts_per_minute TO bla.key_type_counts_per_minute"

      assert statement =~ "FROM bla.source_table_123"
    end

    test "Allows the default view name to be modified" do
      statement =
        QueryTemplates.create_materialized_view_statement("source_table_321",
          view_name: "some_view_name"
        )

      assert statement =~
               "CREATE MATERIALIZED VIEW IF NOT EXISTS some_view_name TO key_type_counts_per_minute"

      assert statement =~ "FROM source_table_321"
    end

    test "Allows the key table name to be modified" do
      statement =
        QueryTemplates.create_materialized_view_statement("source_table_456",
          key_table: "other_key_table_name"
        )

      assert statement =~
               "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_key_type_counts_per_minute TO other_key_table_name"

      assert statement =~ "FROM source_table_456"
    end
  end
end
