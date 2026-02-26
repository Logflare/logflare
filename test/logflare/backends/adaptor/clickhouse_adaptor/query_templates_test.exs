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

  describe "create_simple_table_statement/3" do
    test "dispatches to logs for :log event type" do
      ddl = QueryTemplates.create_simple_table_statement("simple_otel_logs_test", :log, [])
      assert ddl =~ "simple_otel_logs_test"
      assert ddl =~ "Map(LowCardinality(String), String)"
      assert ddl =~ "`log_attributes` Map(String, String)"
    end

    test "dispatches to metrics for :metric event type" do
      ddl = QueryTemplates.create_simple_table_statement("simple_otel_metrics_test", :metric, [])
      assert ddl =~ "simple_otel_metrics_test"
      assert ddl =~ "`attributes` Map(String, String)"
    end

    test "dispatches to traces for :trace event type" do
      ddl = QueryTemplates.create_simple_table_statement("simple_otel_traces_test", :trace, [])
      assert ddl =~ "simple_otel_traces_test"
      assert ddl =~ "`span_attributes` Map(String, String)"
    end
  end

  describe "create_simple_logs_table_statement/2" do
    test "uses Map types instead of JSON for attribute columns" do
      ddl = QueryTemplates.create_simple_logs_table_statement("simple_otel_logs_test")
      assert ddl =~ "`resource_attributes` Map(LowCardinality(String), String) CODEC(ZSTD(1))"
      assert ddl =~ "`scope_attributes` Map(LowCardinality(String), String) CODEC(ZSTD(1))"
      assert ddl =~ "`log_attributes` Map(String, String) CODEC(ZSTD(1))"
      refute ddl =~ "JSON"
    end

    test "preserves all non-attribute columns from standard DDL" do
      ddl = QueryTemplates.create_simple_logs_table_statement("simple_otel_logs_test")
      assert ddl =~ "`id` UUID"
      assert ddl =~ "`severity_text` LowCardinality(String)"
      assert ddl =~ "`event_message` String CODEC(ZSTD(1))"
      assert ddl =~ "`timestamp` DateTime64(9)"
      assert ddl =~ "idx_trace_id"
    end

    test "preserves partitioning, ordering, and settings" do
      ddl = QueryTemplates.create_simple_logs_table_statement("simple_otel_logs_test")
      assert ddl =~ "PARTITION BY toDate(timestamp)"
      assert ddl =~ "ORDER BY (source_name, project, toDate(timestamp))"
      assert ddl =~ "SETTINGS index_granularity = 8192"
    end
  end

  describe "create_simple_metrics_table_statement/2" do
    test "uses Map types instead of JSON for attribute columns" do
      ddl = QueryTemplates.create_simple_metrics_table_statement("simple_otel_metrics_test")
      assert ddl =~ "`resource_attributes` Map(LowCardinality(String), String) CODEC(ZSTD(1))"
      assert ddl =~ "`scope_attributes` Map(LowCardinality(String), String) CODEC(ZSTD(1))"
      assert ddl =~ "`attributes` Map(String, String) CODEC(ZSTD(1))"
      refute ddl =~ "JSON"
    end

    test "uses Array(Map(...)) for exemplars.filtered_attributes" do
      ddl = QueryTemplates.create_simple_metrics_table_statement("simple_otel_metrics_test")

      assert ddl =~
               "`exemplars.filtered_attributes` Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1))"
    end

    test "preserves metric-specific columns" do
      ddl = QueryTemplates.create_simple_metrics_table_statement("simple_otel_metrics_test")
      assert ddl =~ "`metric_type` Enum8"
      assert ddl =~ "`value` Float64"
      assert ddl =~ "`bucket_counts` Array(UInt64)"
      assert ddl =~ "ORDER BY (source_name, metric_name, project, toDate(timestamp))"
    end
  end

  describe "create_simple_traces_table_statement/2" do
    test "uses Map types instead of JSON for attribute columns" do
      ddl = QueryTemplates.create_simple_traces_table_statement("simple_otel_traces_test")
      assert ddl =~ "`resource_attributes` Map(LowCardinality(String), String) CODEC(ZSTD(1))"
      assert ddl =~ "`span_attributes` Map(String, String) CODEC(ZSTD(1))"
      refute ddl =~ "JSON"
    end

    test "uses Array(Map(...)) for events.attributes and links.attributes" do
      ddl = QueryTemplates.create_simple_traces_table_statement("simple_otel_traces_test")

      assert ddl =~
               "`events.attributes` Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1))"

      assert ddl =~
               "`links.attributes` Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1))"
    end

    test "preserves trace-specific columns and indexes" do
      ddl = QueryTemplates.create_simple_traces_table_statement("simple_otel_traces_test")
      assert ddl =~ "`duration` UInt64"
      assert ddl =~ "`span_name` LowCardinality(String)"
      assert ddl =~ "idx_trace_id"
      assert ddl =~ "idx_duration"
      assert ddl =~ "ORDER BY (source_name, span_name, project, toDate(timestamp))"
    end
  end
end
