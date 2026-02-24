defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.QueryTemplates do
  @moduledoc """
  Common query templates utilized by the `ClickHouseAdaptor`.
  """

  import Logflare.Utils.Guards

  alias Logflare.LogEvent.TypeDetection

  @default_table_engine Application.compile_env(:logflare, :clickhouse_backend_adaptor)[:engine]
  @default_ttl_days 90

  @log_columns ~w(id source_uuid source_name project trace_id span_id trace_flags
    severity_text severity_number service_name event_message scope_name scope_version
    scope_schema_url resource_schema_url resource_attributes scope_attributes
    log_attributes mapping_config_id timestamp)

  @metric_columns ~w(id source_uuid source_name project time_unix start_time_unix
    metric_name metric_description metric_unit metric_type service_name event_message
    scope_name scope_version scope_schema_url resource_schema_url resource_attributes
    scope_attributes attributes aggregation_temporality is_monotonic flags value count
    sum min max scale zero_count positive_offset negative_offset
    bucket_counts explicit_bounds positive_bucket_counts negative_bucket_counts
    quantile_values quantiles exemplars.filtered_attributes exemplars.time_unix
    exemplars.value exemplars.span_id exemplars.trace_id
    mapping_config_id timestamp)

  @trace_columns ~w(id source_uuid source_name project trace_id span_id
    parent_span_id trace_state span_name span_kind service_name event_message duration
    status_code status_message scope_name scope_version resource_attributes span_attributes
    events.timestamp events.name events.attributes
    links.trace_id links.span_id links.trace_state links.attributes
    mapping_config_id timestamp)

  @doc """
  Returns the column names for a given event type.
  """
  @spec columns_for_type(TypeDetection.event_type()) :: [String.t()]
  def columns_for_type(:log), do: @log_columns
  def columns_for_type(:metric), do: @metric_columns
  def columns_for_type(:trace), do: @trace_columns

  @doc """
  Generates a ClickHouse query statement to check that the user GRANTs include the needed permissions.

  The results will return a `1` if the user _has_ the needed GRANTs or a `0` otherwise.

  Because this is generally run via a connection that was provided with the
  user credentials and database, there is no need to supply the specific DB by default.

  ###Options

  - `:database` - (Optional) Will produce a fully qualified `<database>.*` string when provided with a value. Defaults to `nil`.

  """
  @spec grant_check_statement(opts :: Keyword.t()) :: String.t()
  def grant_check_statement(opts \\ []) when is_list(opts) do
    database = Keyword.get(opts, :database, nil)

    grant_target_string =
      if is_non_empty_binary(database) do
        "#{database}.*"
      else
        "*"
      end

    "CHECK GRANT CREATE TABLE, ALTER TABLE, INSERT, SELECT, DROP TABLE, CREATE VIEW, DROP VIEW ON #{grant_target_string}"
  end

  @doc """
  Dispatches to the correct type-specific DDL function based on `event_type`.
  """
  @spec create_table_statement(
          table :: String.t(),
          event_type :: TypeDetection.event_type(),
          opts :: Keyword.t()
        ) :: String.t()
  def create_table_statement(table, :log, opts),
    do: create_logs_table_statement(table, opts)

  def create_table_statement(table, :metric, opts),
    do: create_metrics_table_statement(table, opts)

  def create_table_statement(table, :trace, opts),
    do: create_traces_table_statement(table, opts)

  @doc """
  Generates a ClickHouse DDL statement for an OTEL logs table.
  """
  @spec create_logs_table_statement(table :: String.t(), opts :: Keyword.t()) :: String.t()
  def create_logs_table_statement(table, opts \\ [])
      when is_non_empty_binary(table) and is_list(opts) do
    {db_table, engine, ttl_days} = extract_opts(table, opts)

    Enum.join([
      """
      CREATE TABLE IF NOT EXISTS #{db_table} (
        `id` UUID,
        `source_uuid` LowCardinality(String) CODEC(ZSTD(1)),
        `source_name` LowCardinality(String) CODEC(ZSTD(1)),
        `project` String CODEC(ZSTD(1)),
        `trace_id` String CODEC(ZSTD(1)),
        `span_id` String CODEC(ZSTD(1)),
        `trace_flags` UInt8,
        `severity_text` LowCardinality(String) CODEC(ZSTD(1)),
        `severity_number` UInt8,
        `service_name` LowCardinality(String) CODEC(ZSTD(1)),
        `event_message` String CODEC(ZSTD(1)),
        `scope_name` String CODEC(ZSTD(1)),
        `scope_version` LowCardinality(String) CODEC(ZSTD(1)),
        `scope_schema_url` LowCardinality(String) CODEC(ZSTD(1)),
        `resource_schema_url` LowCardinality(String) CODEC(ZSTD(1)),
        `resource_attributes` JSON(max_dynamic_paths=0, max_dynamic_types=1) CODEC(ZSTD(1)),
        `scope_attributes` JSON(max_dynamic_paths=0, max_dynamic_types=1) CODEC(ZSTD(1)),
        `log_attributes` JSON(max_dynamic_paths=0, max_dynamic_types=1) CODEC(ZSTD(1)),
        `mapping_config_id` UUID,
        `timestamp` DateTime64(9) CODEC(Delta(8), ZSTD(1)),
        `timestamp_time` DateTime DEFAULT toDateTime(timestamp),
        INDEX idx_trace_id trace_id TYPE bloom_filter(0.001) GRANULARITY 1
      )
      ENGINE = #{engine}
      PARTITION BY toDate(timestamp)
      ORDER BY (source_name, project, toDate(timestamp))
      """,
      if is_pos_integer(ttl_days) do
        "TTL toDateTime(timestamp) + INTERVAL #{ttl_days} DAY\n"
      end,
      "SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1"
    ])
    |> String.trim_trailing("\n")
  end

  @doc """
  Generates a ClickHouse DDL statement for an OTEL metrics table.
  """
  @spec create_metrics_table_statement(table :: String.t(), opts :: Keyword.t()) :: String.t()
  def create_metrics_table_statement(table, opts \\ [])
      when is_non_empty_binary(table) and is_list(opts) do
    {db_table, engine, ttl_days} = extract_opts(table, opts)

    Enum.join([
      """
      CREATE TABLE IF NOT EXISTS #{db_table} (
        `id` UUID,
        `source_uuid` LowCardinality(String) CODEC(ZSTD(1)),
        `source_name` LowCardinality(String) CODEC(ZSTD(1)),
        `project` String CODEC(ZSTD(1)),
        `time_unix` Nullable(DateTime64(9)) CODEC(Delta(8), ZSTD(1)),
        `start_time_unix` Nullable(DateTime64(9)) CODEC(Delta(8), ZSTD(1)),
        `metric_name` LowCardinality(String) CODEC(ZSTD(1)),
        `metric_description` String CODEC(ZSTD(1)),
        `metric_unit` LowCardinality(String) CODEC(ZSTD(1)),
        `metric_type` Enum8('gauge' = 1, 'sum' = 2, 'histogram' = 3, 'exponential_histogram' = 4, 'summary' = 5),
        `service_name` LowCardinality(String) CODEC(ZSTD(1)),
        `event_message` String CODEC(ZSTD(1)),
        `scope_name` String CODEC(ZSTD(1)),
        `scope_version` LowCardinality(String) CODEC(ZSTD(1)),
        `scope_schema_url` LowCardinality(String) CODEC(ZSTD(1)),
        `resource_schema_url` LowCardinality(String) CODEC(ZSTD(1)),
        `resource_attributes` JSON(max_dynamic_paths=0, max_dynamic_types=1) CODEC(ZSTD(1)),
        `scope_attributes` JSON(max_dynamic_paths=0, max_dynamic_types=1) CODEC(ZSTD(1)),
        `attributes` JSON(max_dynamic_paths=0, max_dynamic_types=1) CODEC(ZSTD(1)),
        `aggregation_temporality` LowCardinality(String) CODEC(ZSTD(1)),
        `is_monotonic` Bool,
        `flags` UInt32 CODEC(ZSTD(1)),
        `value` Float64 CODEC(ZSTD(1)),
        `count` UInt64 CODEC(ZSTD(1)),
        `sum` Float64 CODEC(ZSTD(1)),
        `bucket_counts` Array(UInt64) CODEC(ZSTD(1)),
        `explicit_bounds` Array(Float64) CODEC(ZSTD(1)),
        `min` Float64 CODEC(ZSTD(1)),
        `max` Float64 CODEC(ZSTD(1)),
        `scale` Int32 CODEC(ZSTD(1)),
        `zero_count` UInt64 CODEC(ZSTD(1)),
        `positive_offset` Int32 CODEC(ZSTD(1)),
        `positive_bucket_counts` Array(UInt64) CODEC(ZSTD(1)),
        `negative_offset` Int32 CODEC(ZSTD(1)),
        `negative_bucket_counts` Array(UInt64) CODEC(ZSTD(1)),
        `quantile_values` Array(Float64) CODEC(ZSTD(1)),
        `quantiles` Array(Float64) CODEC(ZSTD(1)),
        `exemplars.filtered_attributes` Array(JSON(max_dynamic_paths=0, max_dynamic_types=1)) CODEC(ZSTD(1)),
        `exemplars.time_unix` Array(DateTime64(9)) CODEC(ZSTD(1)),
        `exemplars.value` Array(Float64) CODEC(ZSTD(1)),
        `exemplars.span_id` Array(String) CODEC(ZSTD(1)),
        `exemplars.trace_id` Array(String) CODEC(ZSTD(1)),
        `mapping_config_id` UUID,
        `timestamp` DateTime64(9) CODEC(Delta(8), ZSTD(1))
      )
      ENGINE = #{engine}
      PARTITION BY toDate(timestamp)
      ORDER BY (source_name, metric_name, project, toDate(timestamp))
      """,
      if is_pos_integer(ttl_days) do
        "TTL toDateTime(timestamp) + INTERVAL #{ttl_days} DAY\n"
      end,
      "SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1"
    ])
    |> String.trim_trailing("\n")
  end

  @doc """
  Generates a ClickHouse DDL statement for an OTEL traces table.
  """
  @spec create_traces_table_statement(table :: String.t(), opts :: Keyword.t()) :: String.t()
  def create_traces_table_statement(table, opts \\ [])
      when is_non_empty_binary(table) and is_list(opts) do
    {db_table, engine, ttl_days} = extract_opts(table, opts)

    Enum.join([
      """
      CREATE TABLE IF NOT EXISTS #{db_table} (
        `id` UUID,
        `source_uuid` LowCardinality(String) CODEC(ZSTD(1)),
        `source_name` LowCardinality(String) CODEC(ZSTD(1)),
        `project` String CODEC(ZSTD(1)),
        `trace_id` String CODEC(ZSTD(1)),
        `span_id` String CODEC(ZSTD(1)),
        `parent_span_id` String CODEC(ZSTD(1)),
        `trace_state` String CODEC(ZSTD(1)),
        `span_name` LowCardinality(String) CODEC(ZSTD(1)),
        `span_kind` LowCardinality(String) CODEC(ZSTD(1)),
        `service_name` LowCardinality(String) CODEC(ZSTD(1)),
        `event_message` String CODEC(ZSTD(1)),
        `duration` UInt64 CODEC(ZSTD(1)),
        `status_code` LowCardinality(String) CODEC(ZSTD(1)),
        `status_message` String CODEC(ZSTD(1)),
        `scope_name` String CODEC(ZSTD(1)),
        `scope_version` String CODEC(ZSTD(1)),
        `resource_attributes` JSON(max_dynamic_paths=0, max_dynamic_types=1) CODEC(ZSTD(1)),
        `span_attributes` JSON(max_dynamic_paths=0, max_dynamic_types=1) CODEC(ZSTD(1)),
        `events.timestamp` Array(DateTime64(9)) CODEC(ZSTD(1)),
        `events.name` Array(LowCardinality(String)) CODEC(ZSTD(1)),
        `events.attributes` Array(JSON(max_dynamic_paths=0, max_dynamic_types=1)) CODEC(ZSTD(1)),
        `links.trace_id` Array(String) CODEC(ZSTD(1)),
        `links.span_id` Array(String) CODEC(ZSTD(1)),
        `links.trace_state` Array(String) CODEC(ZSTD(1)),
        `links.attributes` Array(JSON(max_dynamic_paths=0, max_dynamic_types=1)) CODEC(ZSTD(1)),
        `mapping_config_id` UUID,
        `timestamp` DateTime64(9) CODEC(Delta(8), ZSTD(1)),
        INDEX idx_trace_id trace_id TYPE bloom_filter(0.001) GRANULARITY 1,
        INDEX idx_duration duration TYPE minmax GRANULARITY 1
      )
      ENGINE = #{engine}
      PARTITION BY toDate(timestamp)
      ORDER BY (source_name, span_name, project, toDate(timestamp))
      """,
      if is_pos_integer(ttl_days) do
        "TTL toDateTime(timestamp) + INTERVAL #{ttl_days} DAY\n"
      end,
      "SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1"
    ])
    |> String.trim_trailing("\n")
  end

  @spec extract_opts(String.t(), Keyword.t()) :: {String.t(), String.t(), pos_integer() | nil}
  defp extract_opts(table, opts) do
    database = Keyword.get(opts, :database)
    engine = Keyword.get(opts, :engine, @default_table_engine)
    ttl_days_temp = Keyword.get(opts, :ttl_days, @default_ttl_days)

    ttl_days =
      if is_pos_integer(ttl_days_temp) do
        ttl_days_temp
      end

    db_table =
      if is_non_empty_binary(database) do
        "#{database}.#{table}"
      else
        "#{table}"
      end

    {db_table, engine, ttl_days}
  end
end
