defmodule Logflare.Backends.Adaptor.S3TablesAdaptor.IcebergSchema do
  @moduledoc """
  Iceberg table schemas for the S3 Tables backend, mirroring the ClickHouse OTEL
  tables (`otel_logs`, `otel_metrics`, `otel_traces`) so that ingestion supports
  ClickHouse's current OTEL format.

  Field lists here are kept in lockstep with
  `Logflare.Backends.Adaptor.ClickHouseAdaptor.QueryTemplates.columns_for_type/1`
  (see the drift-guard test in `iceberg_schema_test.exs`) and passed to the
  `s3_tables_ex` NIF to build the actual Iceberg schema.

  ClickHouse `Nested` columns (e.g. `events.timestamp`, `exemplars.value`,
  `links.attributes`) are kept as flat, dotted column names for 1:1 parity —
  this is legal in Iceberg, but query engines that treat `.` as a struct-path
  separator require the identifier to be quoted, e.g. DuckDB needs
  `"events.timestamp"` (unquoted, DuckDB's binder parses the dot as
  table/struct access and errors). Athena/Spark are pickier about this; a
  `list<struct>` remodel would be schema-breaking and is deferred.
  """

  alias Logflare.LogEvent.TypeDetection

  @type field :: %{name: String.t(), type: String.t(), required: boolean()}

  @log_fields [
    %{name: "id", type: "string", required: true},
    %{name: "source_uuid", type: "string", required: true},
    %{name: "source_name", type: "string", required: true},
    %{name: "project", type: "string", required: true},
    %{name: "trace_id", type: "string", required: true},
    %{name: "span_id", type: "string", required: true},
    %{name: "trace_flags", type: "int", required: true},
    %{name: "severity_text", type: "string", required: true},
    %{name: "severity_number", type: "int", required: true},
    %{name: "service_name", type: "string", required: true},
    %{name: "event_message", type: "string", required: true},
    %{name: "scope_name", type: "string", required: true},
    %{name: "scope_version", type: "string", required: true},
    %{name: "scope_schema_url", type: "string", required: true},
    %{name: "resource_schema_url", type: "string", required: true},
    %{name: "resource_attributes", type: "map<string,string>", required: true},
    %{name: "scope_attributes", type: "map<string,string>", required: true},
    %{name: "log_attributes", type: "map<string,string>", required: true},
    %{name: "mapping_config_id", type: "string", required: true},
    %{name: "ingested_at", type: "timestamptz", required: false},
    %{name: "timestamp", type: "timestamptz", required: true}
  ]

  @metric_fields [
    %{name: "id", type: "string", required: true},
    %{name: "source_uuid", type: "string", required: true},
    %{name: "source_name", type: "string", required: true},
    %{name: "project", type: "string", required: true},
    %{name: "time_unix", type: "timestamptz", required: false},
    %{name: "start_time_unix", type: "timestamptz", required: false},
    %{name: "metric_name", type: "string", required: true},
    %{name: "metric_description", type: "string", required: true},
    %{name: "metric_unit", type: "string", required: true},
    %{name: "metric_type", type: "string", required: true},
    %{name: "service_name", type: "string", required: true},
    %{name: "event_message", type: "string", required: true},
    %{name: "scope_name", type: "string", required: true},
    %{name: "scope_version", type: "string", required: true},
    %{name: "scope_schema_url", type: "string", required: true},
    %{name: "resource_schema_url", type: "string", required: true},
    %{name: "resource_attributes", type: "map<string,string>", required: true},
    %{name: "scope_attributes", type: "map<string,string>", required: true},
    %{name: "attributes", type: "map<string,string>", required: true},
    %{name: "aggregation_temporality", type: "string", required: true},
    %{name: "is_monotonic", type: "boolean", required: true},
    %{name: "flags", type: "int", required: true},
    %{name: "value", type: "double", required: true},
    %{name: "count", type: "long", required: true},
    %{name: "sum", type: "double", required: true},
    %{name: "min", type: "double", required: true},
    %{name: "max", type: "double", required: true},
    %{name: "scale", type: "int", required: true},
    %{name: "zero_count", type: "long", required: true},
    %{name: "positive_offset", type: "int", required: true},
    %{name: "negative_offset", type: "int", required: true},
    %{name: "bucket_counts", type: "list<long>", required: true},
    %{name: "explicit_bounds", type: "list<double>", required: true},
    %{name: "positive_bucket_counts", type: "list<long>", required: true},
    %{name: "negative_bucket_counts", type: "list<long>", required: true},
    %{name: "quantile_values", type: "list<double>", required: true},
    %{name: "quantiles", type: "list<double>", required: true},
    %{name: "exemplars.filtered_attributes", type: "list<map<string,string>>", required: true},
    %{name: "exemplars.time_unix", type: "list<timestamptz>", required: true},
    %{name: "exemplars.value", type: "list<double>", required: true},
    %{name: "exemplars.span_id", type: "list<string>", required: true},
    %{name: "exemplars.trace_id", type: "list<string>", required: true},
    %{name: "mapping_config_id", type: "string", required: true},
    %{name: "ingested_at", type: "timestamptz", required: false},
    %{name: "timestamp", type: "timestamptz", required: true}
  ]

  @trace_fields [
    %{name: "id", type: "string", required: true},
    %{name: "source_uuid", type: "string", required: true},
    %{name: "source_name", type: "string", required: true},
    %{name: "project", type: "string", required: true},
    %{name: "trace_id", type: "string", required: true},
    %{name: "span_id", type: "string", required: true},
    %{name: "parent_span_id", type: "string", required: true},
    %{name: "trace_state", type: "string", required: true},
    %{name: "span_name", type: "string", required: true},
    %{name: "span_kind", type: "string", required: true},
    %{name: "service_name", type: "string", required: true},
    %{name: "event_message", type: "string", required: true},
    %{name: "duration", type: "long", required: true},
    %{name: "status_code", type: "string", required: true},
    %{name: "status_message", type: "string", required: true},
    %{name: "scope_name", type: "string", required: true},
    %{name: "scope_version", type: "string", required: true},
    %{name: "resource_attributes", type: "map<string,string>", required: true},
    %{name: "span_attributes", type: "map<string,string>", required: true},
    %{name: "events.timestamp", type: "list<timestamptz>", required: true},
    %{name: "events.name", type: "list<string>", required: true},
    %{name: "events.attributes", type: "list<map<string,string>>", required: true},
    %{name: "links.trace_id", type: "list<string>", required: true},
    %{name: "links.span_id", type: "list<string>", required: true},
    %{name: "links.trace_state", type: "list<string>", required: true},
    %{name: "links.attributes", type: "list<map<string,string>>", required: true},
    %{name: "mapping_config_id", type: "string", required: true},
    %{name: "ingested_at", type: "timestamptz", required: false},
    %{name: "timestamp", type: "timestamptz", required: true}
  ]

  @doc """
  Returns all event types with a corresponding Iceberg table.
  """
  @spec event_types() :: [TypeDetection.event_type()]
  def event_types, do: [:log, :metric, :trace]

  @doc """
  Returns the Iceberg table name for a given event type.
  """
  @spec table_name(TypeDetection.event_type()) :: String.t()
  def table_name(:log), do: "otel_logs"
  def table_name(:metric), do: "otel_metrics"
  def table_name(:trace), do: "otel_traces"

  @doc """
  Returns the ordered column definitions for a given event type's Iceberg table.
  """
  @spec fields(TypeDetection.event_type()) :: [field()]
  def fields(:log), do: @log_fields
  def fields(:metric), do: @metric_fields
  def fields(:trace), do: @trace_fields
end
