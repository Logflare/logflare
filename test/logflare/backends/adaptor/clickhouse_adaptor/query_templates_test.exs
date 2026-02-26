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

  describe "optimized and cloud settings in DDL output" do
    test "logs DDL includes optimized settings when opt is passed" do
      ddl = QueryTemplates.create_logs_table_statement("test", optimized_settings: true)
      assert ddl =~ "max_parts_in_total = 20000"
      assert ddl =~ "parts_to_delay_insert = 5000"
      assert ddl =~ "parts_to_throw_insert = 10000"
      assert ddl =~ "min_bytes_for_wide_part = 0"
      assert ddl =~ "max_parts_to_merge_at_once = 25"
      refute ddl =~ "shared_merge_tree"
    end

    test "simple logs DDL includes optimized settings when opt is passed" do
      ddl = QueryTemplates.create_simple_logs_table_statement("test", optimized_settings: true)
      assert ddl =~ "max_parts_in_total = 20000"
      refute ddl =~ "shared_merge_tree"
    end

    test "logs DDL includes cloud settings when both opts are passed" do
      ddl =
        QueryTemplates.create_logs_table_statement("test",
          optimized_settings: true,
          clickhouse_cloud: true
        )

      assert ddl =~ "max_parts_in_total = 20000"
      assert ddl =~ "shared_merge_tree_enable_coordinated_merges = 1"
      assert ddl =~ "min_bytes_for_full_part_storage = 2147483648"
    end

    test "DDL excludes optimized and cloud settings by default" do
      ddl = QueryTemplates.create_logs_table_statement("test")
      assert ddl =~ "SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1"
      refute ddl =~ "max_parts_in_total"
      refute ddl =~ "shared_merge_tree"
    end

    test "metrics/traces DDL excludes optimized settings even with opts (controlled by module attr)" do
      ddl = QueryTemplates.create_metrics_table_statement("test")
      refute ddl =~ "max_parts_in_total"
    end

    test "cloud settings are not applied without optimized_settings flag" do
      ddl = QueryTemplates.create_logs_table_statement("test", clickhouse_cloud: true)
      refute ddl =~ "shared_merge_tree"
      refute ddl =~ "max_parts_in_total"
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
