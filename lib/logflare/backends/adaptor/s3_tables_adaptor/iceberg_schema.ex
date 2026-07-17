defmodule Logflare.Backends.Adaptor.S3TablesAdaptor.IcebergSchema do
  @moduledoc """
  Iceberg table schemas for the S3 Tables backend, mirroring the ClickHouse OTEL
  tables (`otel_logs`, `otel_metrics`, `otel_traces`) so that ingestion supports
  ClickHouse's current OTEL format.

  ClickHouse `Nested` columns (e.g. `events.timestamp`, `exemplars.value`,
  `links.attributes`) are kept as flat, dotted column names for 1:1 parity —
  this is legal in Iceberg, but query engines that treat `.` as a struct-path
  separator require the identifier to be quoted, e.g. DuckDB needs
  `"events.timestamp"`.

  Only `id` and `timestamp` are required. Each table is stamped with a
  `logflare.schema-version` property (see `table_properties/0`) so future
  provisioning runs can detect schema drift and drive migrations.
  """

  alias Logflare.LogEvent.TypeDetection

  @schema_version "1"

  # keeps the iceberg-rust built-in commit retry loop well under the
  # append NIF timeout (see `Native.append_batch/3`)
  @commit_retry_total_timeout_ms "30000"

  @type field :: %{name: String.t(), type: String.t(), required: boolean()}

  @log_fields [
    %{name: "id", type: "string", required: true},
    %{name: "source_uuid", type: "string", required: false},
    %{name: "source_name", type: "string", required: false},
    %{name: "project", type: "string", required: false},
    %{name: "trace_id", type: "string", required: false},
    %{name: "span_id", type: "string", required: false},
    %{name: "trace_flags", type: "int", required: false},
    %{name: "severity_text", type: "string", required: false},
    %{name: "severity_number", type: "int", required: false},
    %{name: "service_name", type: "string", required: false},
    %{name: "event_message", type: "string", required: false},
    %{name: "scope_name", type: "string", required: false},
    %{name: "scope_version", type: "string", required: false},
    %{name: "scope_schema_url", type: "string", required: false},
    %{name: "resource_schema_url", type: "string", required: false},
    %{name: "resource_attributes", type: "map<string,string>", required: false},
    %{name: "scope_attributes", type: "map<string,string>", required: false},
    %{name: "log_attributes", type: "map<string,string>", required: false},
    %{name: "mapping_config_id", type: "string", required: false},
    %{name: "ingested_at", type: "timestamptz", required: false},
    %{name: "timestamp", type: "timestamptz", required: true}
  ]

  @metric_fields [
    %{name: "id", type: "string", required: true},
    %{name: "source_uuid", type: "string", required: false},
    %{name: "source_name", type: "string", required: false},
    %{name: "project", type: "string", required: false},
    %{name: "time_unix", type: "timestamptz", required: false},
    %{name: "start_time_unix", type: "timestamptz", required: false},
    %{name: "metric_name", type: "string", required: false},
    %{name: "metric_description", type: "string", required: false},
    %{name: "metric_unit", type: "string", required: false},
    %{name: "metric_type", type: "string", required: false},
    %{name: "service_name", type: "string", required: false},
    %{name: "event_message", type: "string", required: false},
    %{name: "scope_name", type: "string", required: false},
    %{name: "scope_version", type: "string", required: false},
    %{name: "scope_schema_url", type: "string", required: false},
    %{name: "resource_schema_url", type: "string", required: false},
    %{name: "resource_attributes", type: "map<string,string>", required: false},
    %{name: "scope_attributes", type: "map<string,string>", required: false},
    %{name: "attributes", type: "map<string,string>", required: false},
    %{name: "aggregation_temporality", type: "string", required: false},
    %{name: "is_monotonic", type: "boolean", required: false},
    %{name: "flags", type: "int", required: false},
    %{name: "value", type: "double", required: false},
    %{name: "count", type: "long", required: false},
    %{name: "sum", type: "double", required: false},
    %{name: "min", type: "double", required: false},
    %{name: "max", type: "double", required: false},
    %{name: "scale", type: "int", required: false},
    %{name: "zero_count", type: "long", required: false},
    %{name: "positive_offset", type: "int", required: false},
    %{name: "negative_offset", type: "int", required: false},
    %{name: "bucket_counts", type: "list<long>", required: false},
    %{name: "explicit_bounds", type: "list<double>", required: false},
    %{name: "positive_bucket_counts", type: "list<long>", required: false},
    %{name: "negative_bucket_counts", type: "list<long>", required: false},
    %{name: "quantile_values", type: "list<double>", required: false},
    %{name: "quantiles", type: "list<double>", required: false},
    %{name: "exemplars.filtered_attributes", type: "list<map<string,string>>", required: false},
    %{name: "exemplars.time_unix", type: "list<timestamptz>", required: false},
    %{name: "exemplars.value", type: "list<double>", required: false},
    %{name: "exemplars.span_id", type: "list<string>", required: false},
    %{name: "exemplars.trace_id", type: "list<string>", required: false},
    %{name: "mapping_config_id", type: "string", required: false},
    %{name: "ingested_at", type: "timestamptz", required: false},
    %{name: "timestamp", type: "timestamptz", required: true}
  ]

  @trace_fields [
    %{name: "id", type: "string", required: true},
    %{name: "source_uuid", type: "string", required: false},
    %{name: "source_name", type: "string", required: false},
    %{name: "project", type: "string", required: false},
    %{name: "trace_id", type: "string", required: false},
    %{name: "span_id", type: "string", required: false},
    %{name: "parent_span_id", type: "string", required: false},
    %{name: "trace_state", type: "string", required: false},
    %{name: "span_name", type: "string", required: false},
    %{name: "span_kind", type: "string", required: false},
    %{name: "service_name", type: "string", required: false},
    %{name: "event_message", type: "string", required: false},
    %{name: "duration", type: "long", required: false},
    %{name: "status_code", type: "string", required: false},
    %{name: "status_message", type: "string", required: false},
    %{name: "scope_name", type: "string", required: false},
    %{name: "scope_version", type: "string", required: false},
    %{name: "resource_attributes", type: "map<string,string>", required: false},
    %{name: "span_attributes", type: "map<string,string>", required: false},
    %{name: "events.timestamp", type: "list<timestamptz>", required: false},
    %{name: "events.name", type: "list<string>", required: false},
    %{name: "events.attributes", type: "list<map<string,string>>", required: false},
    %{name: "links.trace_id", type: "list<string>", required: false},
    %{name: "links.span_id", type: "list<string>", required: false},
    %{name: "links.trace_state", type: "list<string>", required: false},
    %{name: "links.attributes", type: "list<map<string,string>>", required: false},
    %{name: "mapping_config_id", type: "string", required: false},
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

  @doc """
  Returns the Iceberg table properties stamped on every table at creation.
  """
  @spec table_properties() :: %{String.t() => String.t()}
  def table_properties do
    %{
      "logflare.schema-version" => @schema_version,
      "commit.retry.total-timeout-ms" => @commit_retry_total_timeout_ms
    }
  end
end
