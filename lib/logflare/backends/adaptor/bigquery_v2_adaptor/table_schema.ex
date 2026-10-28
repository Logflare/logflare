defmodule Logflare.Backends.Adaptor.BigQueryV2Adaptor.TableSchema do
  @moduledoc """
  Fixed BigQuery table schemas for the `bigquery_v2` backend, mirroring the
  ClickHouse OTEL tables (`otel_logs`, `otel_metrics`, `otel_traces`).

  All sources of a backend share these tables, so `project` and `source_uuid`
  carry the tenancy of every row and are `REQUIRED` alongside `id` and
  `timestamp`. Rows are partitioned by day on `timestamp` and clustered by
  `(project, source_uuid, timestamp)`.

  Rows have at most one level of nesting:

    * Attribute maps are stored as `ARRAY<STRUCT<key STRING, value STRING>>`, the
      BigQuery equivalent of ClickHouse `Map(String, String)` and Iceberg
      `map<string,string>`.
    * ClickHouse `Nested` columns (`events.*`, `links.*`, `exemplars.*`) are kept
      as parallel arrays, with `.` replaced by `_` because BigQuery column names
      cannot contain `.`. Their per-element attribute maps
      (`events_attributes`, `links_attributes`, `exemplars_filtered_attributes`)
      are `ARRAY<STRING>` of JSON objects, since BigQuery has no nested arrays.

  Timestamps and `duration` are in microseconds, the precision the mapper's
  NDJSON output narrows to. Timestamp lists (`events_timestamp`,
  `exemplars_time_unix`) are `ARRAY<INT64>` of Unix microseconds, because BigQuery
  fails Arrow appends to `ARRAY<TIMESTAMP>` columns with an internal error; query
  them with `TIMESTAMP_MICROS`.

  Each table carries a `logflare_schema_version` label: a truncated SHA-256
  hash of its field definitions and layout. Drift between a live table and
  this module is only reported; changing anything here means existing tables
  must be dropped and recreated.
  """

  alias GoogleApi.BigQuery.V2.Model
  alias Logflare.Backends.Adaptor.BigQueryAdaptor.ArrowIPC
  alias Logflare.LogEvent.TypeDetection

  @type field_type :: ArrowIPC.field_type()
  @type field :: %{name: String.t(), type: field_type(), required: boolean()}
  @type layout :: %{partition_field: String.t(), clustering: [String.t()]}

  @schema_version_label "logflare_schema_version"
  @schema_version_length 32

  @layout %{
    partition_field: "timestamp",
    clustering: ["project", "source_uuid", "timestamp"]
  }

  @event_types [:log, :metric, :trace]

  @log_fields [
    %{name: "id", type: :string, required: true},
    %{name: "source_uuid", type: :string, required: true},
    %{name: "source_name", type: :string, required: false},
    %{name: "project", type: :string, required: true},
    %{name: "trace_id", type: :string, required: false},
    %{name: "span_id", type: :string, required: false},
    %{name: "trace_flags", type: :integer, required: false},
    %{name: "severity_text", type: :string, required: false},
    %{name: "severity_number", type: :integer, required: false},
    %{name: "service_name", type: :string, required: false},
    %{name: "event_message", type: :string, required: false},
    %{name: "scope_name", type: :string, required: false},
    %{name: "scope_version", type: :string, required: false},
    %{name: "scope_schema_url", type: :string, required: false},
    %{name: "resource_schema_url", type: :string, required: false},
    %{name: "resource_attributes", type: :string_map, required: false},
    %{name: "scope_attributes", type: :string_map, required: false},
    %{name: "log_attributes", type: :string_map, required: false},
    %{name: "mapping_config_id", type: :string, required: false},
    %{name: "ingested_at", type: :timestamp, required: false},
    %{name: "timestamp", type: :timestamp, required: true}
  ]

  @metric_fields [
    %{name: "id", type: :string, required: true},
    %{name: "source_uuid", type: :string, required: true},
    %{name: "source_name", type: :string, required: false},
    %{name: "project", type: :string, required: true},
    %{name: "time_unix", type: :timestamp, required: false},
    %{name: "start_time_unix", type: :timestamp, required: false},
    %{name: "metric_name", type: :string, required: false},
    %{name: "metric_description", type: :string, required: false},
    %{name: "metric_unit", type: :string, required: false},
    %{name: "metric_type", type: :string, required: false},
    %{name: "service_name", type: :string, required: false},
    %{name: "event_message", type: :string, required: false},
    %{name: "scope_name", type: :string, required: false},
    %{name: "scope_version", type: :string, required: false},
    %{name: "scope_schema_url", type: :string, required: false},
    %{name: "resource_schema_url", type: :string, required: false},
    %{name: "resource_attributes", type: :string_map, required: false},
    %{name: "scope_attributes", type: :string_map, required: false},
    %{name: "attributes", type: :string_map, required: false},
    %{name: "aggregation_temporality", type: :string, required: false},
    %{name: "is_monotonic", type: :boolean, required: false},
    %{name: "flags", type: :integer, required: false},
    %{name: "value", type: :float, required: false},
    %{name: "count", type: :integer, required: false},
    %{name: "sum", type: :float, required: false},
    %{name: "min", type: :float, required: false},
    %{name: "max", type: :float, required: false},
    %{name: "scale", type: :integer, required: false},
    %{name: "zero_count", type: :integer, required: false},
    %{name: "positive_offset", type: :integer, required: false},
    %{name: "negative_offset", type: :integer, required: false},
    %{name: "bucket_counts", type: :integer_array, required: false},
    %{name: "explicit_bounds", type: :float_array, required: false},
    %{name: "positive_bucket_counts", type: :integer_array, required: false},
    %{name: "negative_bucket_counts", type: :integer_array, required: false},
    %{name: "quantile_values", type: :float_array, required: false},
    %{name: "quantiles", type: :float_array, required: false},
    %{name: "exemplars_filtered_attributes", type: :json_array, required: false},
    %{name: "exemplars_time_unix", type: :integer_array, required: false},
    %{name: "exemplars_value", type: :float_array, required: false},
    %{name: "exemplars_span_id", type: :string_array, required: false},
    %{name: "exemplars_trace_id", type: :string_array, required: false},
    %{name: "mapping_config_id", type: :string, required: false},
    %{name: "ingested_at", type: :timestamp, required: false},
    %{name: "timestamp", type: :timestamp, required: true}
  ]

  @trace_fields [
    %{name: "id", type: :string, required: true},
    %{name: "source_uuid", type: :string, required: true},
    %{name: "source_name", type: :string, required: false},
    %{name: "project", type: :string, required: true},
    %{name: "trace_id", type: :string, required: false},
    %{name: "span_id", type: :string, required: false},
    %{name: "parent_span_id", type: :string, required: false},
    %{name: "trace_state", type: :string, required: false},
    %{name: "span_name", type: :string, required: false},
    %{name: "span_kind", type: :string, required: false},
    %{name: "service_name", type: :string, required: false},
    %{name: "event_message", type: :string, required: false},
    %{name: "duration", type: :integer, required: false},
    %{name: "status_code", type: :string, required: false},
    %{name: "status_message", type: :string, required: false},
    %{name: "scope_name", type: :string, required: false},
    %{name: "scope_version", type: :string, required: false},
    %{name: "resource_attributes", type: :string_map, required: false},
    %{name: "span_attributes", type: :string_map, required: false},
    %{name: "events_timestamp", type: :integer_array, required: false},
    %{name: "events_name", type: :string_array, required: false},
    %{name: "events_attributes", type: :json_array, required: false},
    %{name: "links_trace_id", type: :string_array, required: false},
    %{name: "links_span_id", type: :string_array, required: false},
    %{name: "links_trace_state", type: :string_array, required: false},
    %{name: "links_attributes", type: :json_array, required: false},
    %{name: "mapping_config_id", type: :string, required: false},
    %{name: "ingested_at", type: :timestamp, required: false},
    %{name: "timestamp", type: :timestamp, required: true}
  ]

  schema_version = fn event_type, fields ->
    field_names = Enum.map(fields, & &1.name)

    for name <- [@layout.partition_field | @layout.clustering], name not in field_names do
      raise "#{event_type} table layout references unknown column #{inspect(name)}"
    end

    fields
    |> Enum.map(fn %{name: name, type: type, required: required} ->
      "#{name}:#{type}:#{required}"
    end)
    |> Enum.concat([
      "partition:#{@layout.partition_field}:day",
      "cluster:#{Enum.join(@layout.clustering, ",")}"
    ])
    |> Enum.join("\n")
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
    |> binary_part(0, @schema_version_length)
  end

  @log_schema_version schema_version.(:log, @log_fields)
  @metric_schema_version schema_version.(:metric, @metric_fields)
  @trace_schema_version schema_version.(:trace, @trace_fields)

  @doc """
  Returns all event types with a provisioned table.
  """
  @spec event_types() :: [TypeDetection.event_type()]
  def event_types, do: @event_types

  @doc """
  Returns the table name for a given event type.
  """
  @spec table_name(TypeDetection.event_type()) :: String.t()
  def table_name(:log), do: "otel_logs"
  def table_name(:metric), do: "otel_metrics"
  def table_name(:trace), do: "otel_traces"

  @doc """
  Returns the ordered column definitions for a given event type's table.
  """
  @spec fields(TypeDetection.event_type()) :: [field()]
  def fields(:log), do: @log_fields
  def fields(:metric), do: @metric_fields
  def fields(:trace), do: @trace_fields

  @doc """
  Returns a table's columns as `{name, type, required}` tuples, the schema format
  `BigQueryAdaptor.ArrowIPC.get_ipc_bytes_with_schema/3` encodes rows against.
  """
  @spec arrow_fields(TypeDetection.event_type()) :: [ArrowIPC.field_spec()]
  def arrow_fields(event_type) do
    Enum.map(fields(event_type), &{&1.name, &1.type, &1.required})
  end

  @doc """
  Returns the partitioning and clustering applied to a table at creation.
  """
  @spec layout(TypeDetection.event_type()) :: layout()
  def layout(event_type) when event_type in @event_types, do: @layout

  @doc """
  Returns the schema version for a given event type's table, a truncated
  SHA-256 hash of its field definitions and layout that fits a BigQuery label value.
  """
  @spec schema_version(TypeDetection.event_type()) :: String.t()
  def schema_version(:log), do: @log_schema_version
  def schema_version(:metric), do: @metric_schema_version
  def schema_version(:trace), do: @trace_schema_version

  @doc """
  Returns the name of the table label holding the schema version.
  """
  @spec schema_version_label() :: String.t()
  def schema_version_label, do: @schema_version_label

  @doc """
  Returns the labels stamped on a table at creation.
  """
  @spec labels(TypeDetection.event_type()) :: %{String.t() => String.t()}
  def labels(event_type) do
    %{
      "managed_by" => "logflare",
      @schema_version_label => schema_version(event_type)
    }
  end

  @doc """
  Builds the BigQuery API table definition for a given event type.
  """
  @spec to_table(TypeDetection.event_type(), project_id :: String.t(), dataset_id :: String.t()) ::
          Model.Table.t()
  def to_table(event_type, project_id, dataset_id) do
    layout = layout(event_type)

    %Model.Table{
      tableReference: %Model.TableReference{
        projectId: project_id,
        datasetId: dataset_id,
        tableId: table_name(event_type)
      },
      schema: %Model.TableSchema{fields: Enum.map(fields(event_type), &to_field_schema/1)},
      timePartitioning: %Model.TimePartitioning{field: layout.partition_field, type: "DAY"},
      requirePartitionFilter: true,
      clustering: %Model.Clustering{fields: layout.clustering},
      description: "Managed by Logflare",
      labels: labels(event_type)
    }
  end

  @spec to_field_schema(field()) :: Model.TableFieldSchema.t()
  defp to_field_schema(%{name: name, type: :string_map}) do
    %Model.TableFieldSchema{
      name: name,
      type: "RECORD",
      mode: "REPEATED",
      fields: [
        %Model.TableFieldSchema{name: "key", type: "STRING", mode: "REQUIRED"},
        %Model.TableFieldSchema{name: "value", type: "STRING", mode: "NULLABLE"}
      ]
    }
  end

  defp to_field_schema(%{name: name, type: type, required: required}) do
    case array_item_type(type) do
      nil ->
        %Model.TableFieldSchema{
          name: name,
          type: bq_type(type),
          mode: if(required, do: "REQUIRED", else: "NULLABLE")
        }

      item_type ->
        %Model.TableFieldSchema{name: name, type: bq_type(item_type), mode: "REPEATED"}
    end
  end

  @spec array_item_type(field_type()) :: field_type() | nil
  defp array_item_type(:integer_array), do: :integer
  defp array_item_type(:float_array), do: :float
  defp array_item_type(:string_array), do: :string
  defp array_item_type(:json_array), do: :string
  defp array_item_type(_type), do: nil

  @spec bq_type(field_type()) :: String.t()
  defp bq_type(:string), do: "STRING"
  defp bq_type(:integer), do: "INT64"
  defp bq_type(:float), do: "FLOAT64"
  defp bq_type(:boolean), do: "BOOL"
  defp bq_type(:timestamp), do: "TIMESTAMP"
end
