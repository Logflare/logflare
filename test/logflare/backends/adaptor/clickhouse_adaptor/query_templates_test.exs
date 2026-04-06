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

  describe "create_table_statement/3" do
    test "dispatches to logs for :log event type" do
      ddl = QueryTemplates.create_table_statement("otel_logs_test", :log, [])
      assert ddl =~ "otel_logs_test"
      assert ddl =~ "Map(LowCardinality(String), String)"
      assert ddl =~ "`log_attributes` Map(String, String)"
    end

    test "dispatches to metrics for :metric event type" do
      ddl = QueryTemplates.create_table_statement("otel_metrics_test", :metric, [])
      assert ddl =~ "otel_metrics_test"
      assert ddl =~ "`attributes` Map(String, String)"
    end

    test "dispatches to traces for :trace event type" do
      ddl = QueryTemplates.create_table_statement("otel_traces_test", :trace, [])
      assert ddl =~ "otel_traces_test"
      assert ddl =~ "`span_attributes` Map(String, String)"
    end
  end

  describe "create_logs_table_statement/2" do
    test "uses Map types for attribute columns" do
      ddl = QueryTemplates.create_logs_table_statement("otel_logs_test")
      assert ddl =~ "`resource_attributes` Map(LowCardinality(String), String) CODEC(ZSTD(1))"
      assert ddl =~ "`scope_attributes` Map(LowCardinality(String), String) CODEC(ZSTD(1))"
      assert ddl =~ "`log_attributes` Map(String, String) CODEC(ZSTD(1))"
      refute ddl =~ "JSON"
    end

    test "preserves all non-attribute columns" do
      ddl = QueryTemplates.create_logs_table_statement("otel_logs_test")
      assert ddl =~ "`id` UUID"
      assert ddl =~ "`severity_text` LowCardinality(String)"
      assert ddl =~ "`event_message` String CODEC(ZSTD(1))"
      assert ddl =~ "`ingested_at` Nullable(DateTime64(6)) CODEC(Delta(8), ZSTD(1))"
      assert ddl =~ "`timestamp` DateTime64(9)"
      assert ddl =~ "`timestamp_time` DateTime DEFAULT toDateTime(timestamp)"
      assert ddl =~ "idx_trace_id"
    end

    test "uses correct partitioning and ordering" do
      ddl = QueryTemplates.create_logs_table_statement("otel_logs_test")
      assert ddl =~ "PARTITION BY toDate(timestamp)"
      assert ddl =~ "PRIMARY KEY (project, source_name, toDateTime(timestamp))"
      assert ddl =~ "ORDER BY (project, source_name, toDateTime(timestamp), timestamp)"
      assert ddl =~ "SETTINGS index_granularity = 8192"
      refute ddl =~ "timestamp_hour"
    end
  end

  describe "create_metrics_table_statement/2" do
    test "uses Map types for attribute columns" do
      ddl = QueryTemplates.create_metrics_table_statement("otel_metrics_test")
      assert ddl =~ "`resource_attributes` Map(LowCardinality(String), String) CODEC(ZSTD(1))"
      assert ddl =~ "`scope_attributes` Map(LowCardinality(String), String) CODEC(ZSTD(1))"
      assert ddl =~ "`attributes` Map(String, String) CODEC(ZSTD(1))"
      refute ddl =~ "JSON"
    end

    test "uses Array(Map(...)) for exemplars.filtered_attributes" do
      ddl = QueryTemplates.create_metrics_table_statement("otel_metrics_test")

      assert ddl =~
               "`exemplars.filtered_attributes` Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1))"
    end

    test "preserves metric-specific columns and ordering" do
      ddl = QueryTemplates.create_metrics_table_statement("otel_metrics_test")
      assert ddl =~ "`metric_type` Enum8"
      assert ddl =~ "`value` Float64"
      assert ddl =~ "`bucket_counts` Array(UInt64)"
      assert ddl =~ "`ingested_at` Nullable(DateTime64(6)) CODEC(Delta(8), ZSTD(1))"
      assert ddl =~ "PRIMARY KEY (project, source_name, toDateTime(timestamp))"
      assert ddl =~ "ORDER BY (project, source_name, toDateTime(timestamp), timestamp)"
      refute ddl =~ "timestamp_hour"
    end
  end

  describe "create_traces_table_statement/2" do
    test "uses Map types for attribute columns" do
      ddl = QueryTemplates.create_traces_table_statement("otel_traces_test")
      assert ddl =~ "`resource_attributes` Map(LowCardinality(String), String) CODEC(ZSTD(1))"
      assert ddl =~ "`span_attributes` Map(String, String) CODEC(ZSTD(1))"
      refute ddl =~ "JSON"
    end

    test "uses Array(Map(...)) for events.attributes and links.attributes" do
      ddl = QueryTemplates.create_traces_table_statement("otel_traces_test")

      assert ddl =~
               "`events.attributes` Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1))"

      assert ddl =~
               "`links.attributes` Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1))"
    end

    test "includes HyperDX/Clickstack Events.* ALIAS columns" do
      ddl = QueryTemplates.create_traces_table_statement("otel_traces_test")

      assert ddl =~ "`Events.Timestamp` Array(DateTime64(9)) ALIAS `events.timestamp`"
      assert ddl =~ "`Events.Name` Array(LowCardinality(String)) ALIAS `events.name`"

      assert ddl =~
               "`Events.Attributes` Array(Map(LowCardinality(String), String)) ALIAS `events.attributes`"
    end

    test "preserves trace-specific columns, indexes, and ordering" do
      ddl = QueryTemplates.create_traces_table_statement("otel_traces_test")
      assert ddl =~ "`duration` UInt64"
      assert ddl =~ "`span_name` LowCardinality(String)"
      assert ddl =~ "`ingested_at` Nullable(DateTime64(6)) CODEC(Delta(8), ZSTD(1))"
      assert ddl =~ "idx_trace_id"
      assert ddl =~ "idx_duration"
      assert ddl =~ "PRIMARY KEY (project, source_name, toDateTime(timestamp))"
      assert ddl =~ "ORDER BY (project, source_name, toDateTime(timestamp), timestamp)"
      refute ddl =~ "timestamp_hour"
    end
  end

  describe "cloud settings in DDL output" do
    test "logs DDL includes cloud settings when opt is passed" do
      ddl = QueryTemplates.create_logs_table_statement("test", clickhouse_cloud: true)
      assert ddl =~ "shared_merge_tree_enable_coordinated_merges = 1"
      assert ddl =~ "min_bytes_for_full_part_storage = 2147483648"
    end

    test "DDL excludes cloud settings by default" do
      ddl = QueryTemplates.create_logs_table_statement("test")
      assert ddl =~ "SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1"
      refute ddl =~ "shared_merge_tree"
    end
  end
end
