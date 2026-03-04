defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.MappingDefaults do
  @moduledoc """
  Default OTEL-aligned mapping configurations for ClickHouse event types.

  Defines how raw log event bodies are transformed into structured schemas
  before RowBinary encoding. Each event type (log, metric, trace) has its own
  field mapping with coalesced path resolution, defaults, and transforms.
  """

  alias Logflare.LogEvent.TypeDetection
  alias Logflare.Mapper.MappingConfig
  alias Logflare.Mapper.MappingConfig.FieldConfig, as: Field
  alias Logflare.Mapper.MappingConfig.InferCondition
  alias Logflare.Mapper.MappingConfig.InferRule

  @log_config_id "00000000-0000-0000-0001-000000000002"
  @metric_config_id "00000000-0000-0000-0002-000000000002"
  @trace_config_id "00000000-0000-0000-0003-000000000002"
  @simple_log_config_id "00000000-0000-0000-0001-000000000003"
  @simple_metric_config_id "00000000-0000-0000-0002-000000000003"
  @simple_trace_config_id "00000000-0000-0000-0003-000000000003"

  @spec config_id(TypeDetection.event_type()) :: String.t()
  def config_id(:log), do: @log_config_id
  def config_id(:metric), do: @metric_config_id
  def config_id(:trace), do: @trace_config_id

  @spec config_id_simple(TypeDetection.event_type()) :: String.t()
  def config_id_simple(:log), do: @simple_log_config_id
  def config_id_simple(:metric), do: @simple_metric_config_id
  def config_id_simple(:trace), do: @simple_trace_config_id

  @spec for_type(TypeDetection.event_type()) :: MappingConfig.t()
  def for_type(:log), do: for_log()
  def for_type(:metric), do: for_metric()
  def for_type(:trace), do: for_trace()

  @spec for_type_simple(TypeDetection.event_type()) :: MappingConfig.t()
  def for_type_simple(:log), do: for_log_simple()
  def for_type_simple(:metric), do: for_metric_simple()
  def for_type_simple(:trace), do: for_trace_simple()

  @spec for_log() :: MappingConfig.t()
  def for_log do
    MappingConfig.new([
      Field.string("project",
        paths: [
          "$.project",
          "$.project_ref",
          "$.project_id",
          "$.metadata.project",
          "$.metadata.tenant",
          "$.metadata.tenantId"
        ],
        default: ""
      ),
      Field.string("trace_id",
        paths: ["$.trace_id", "$.traceId", "$.otel_trace_id", "$.metadata.otel_trace_id"],
        default: ""
      ),
      Field.string("span_id",
        paths: ["$.span_id", "$.spanId", "$.otel_span_id", "$.metadata.otel_span_id"],
        default: ""
      ),
      Field.uint8("trace_flags",
        paths: ["$.trace_flags", "$.traceFlags", "$.flags", "$.metadata.otel_trace_flags"],
        default: 0
      ),
      Field.string("severity_text",
        paths: [
          "$.severity_text",
          "$.severityText",
          "$.metadata.level",
          "$.level",
          "$.metadata.parsed.error_severity"
        ],
        default: "INFO",
        transform: "upcase",
        allowed_values:
          ~w(TRACE DEBUG INFO NOTICE WARN WARNING ERROR FATAL CRITICAL EMERGENCY ALERT LOG PANIC)
      ),
      Field.uint8("severity_number_alt",
        paths: ["$.severity_number", "$.severityNumber"],
        default: 0
      ),
      Field.uint8("severity_number",
        from_output: "severity_text",
        value_map: %{
          "TRACE" => 1,
          "DEBUG" => 5,
          "INFO" => 9,
          "NOTICE" => 11,
          "WARN" => 13,
          "WARNING" => 13,
          "ERROR" => 17,
          "LOG" => 9,
          "FATAL" => 21,
          "CRITICAL" => 21,
          "EMERGENCY" => 21,
          "ALERT" => 21,
          "PANIC" => 21
        },
        default: 0
      ),
      Field.string("service_name",
        paths: [
          "$.resource.service.name",
          "$.service_name",
          "$.resource.name",
          "$.metadata.context.application",
          "$.metadata.context.service",
          "$.SYSLOG_IDENTIFIER",
          "$._SYSTEMD_UNIT"
        ],
        default: ""
      ),
      Field.string("event_message",
        paths: ["$.event_message", "$.message", "$.body", "$.msg"],
        default: ""
      ),
      Field.string("scope_name",
        paths: [
          "$.scope.name",
          "$.metadata.context.module",
          "$.metadata.context.application",
          "$.instrumentation_library.name",
          "$.metadata.namespace",
          "$.metadata.module_path"
        ],
        default: ""
      ),
      Field.string("scope_version",
        paths: [
          "$.scope.version",
          "$.instrumentation_library.version"
        ],
        default: ""
      ),
      Field.string("scope_schema_url",
        paths: ["$.scope.schema_url"],
        default: ""
      ),
      Field.string("resource_schema_url",
        paths: ["$.resource.schema_url"],
        default: ""
      ),
      Field.json("resource_attributes",
        paths: ["$.resource"],
        pick: [
          {"application_id", ["$.app_id", "$.application_id", "$.metadata.app_id"]},
          {"application_name",
           [
             "$.metadata.context.application",
             "$.app_name",
             "$.application_name",
             "$.metadata.app_name",
             "$.metadata.parsed.application_name"
           ]},
          {"cluster", ["$.metadata.cluster", "$.cluster", "$.resource.cluster"]},
          {"host", ["$.metadata.host", "$.metadata.context.host", "$.host"]},
          {"instance_id", ["$.metadata.instance_id"]},
          {"machine_id", ["$.machine_id"]},
          {"node", ["$.metadata.context.vm.node", "$.resource.node"]},
          {"organization_id", ["$.organization_id", "$.org_id"]},
          {"organization_slug", ["$.organization_slug"]},
          {"project",
           [
             "$.project",
             "$.project_ref",
             "$.project_id",
             "$.metadata.project",
             "$.metadata.tenant",
             "$.metadata.tenantId"
           ]},
          {"region", ["$.metadata.region", "$.region"]},
          {"service_name", ["$.resource.service.name", "$.service_name"]},
          {"vector_file", ["$.metadata.vector_file"]},
          {"vector_host", ["$.metadata.vector_host"]}
        ],
        default: %{}
      ),
      Field.json("scope_attributes",
        paths: ["$.scope.attributes", "$.scope"],
        default: %{}
      ),
      Field.json("log_attributes",
        path: "$",
        exclude_keys: ["id", "event_message", "timestamp"],
        elevate_keys: ["metadata"]
      ),
      Field.datetime64("timestamp", path: "$.timestamp", precision: 9)
    ])
  end

  @spec for_metric() :: MappingConfig.t()
  def for_metric do
    MappingConfig.new([
      Field.string("project",
        paths: [
          "$.project",
          "$.project_ref",
          "$.project_id",
          "$.metadata.project",
          "$.metadata.tenant",
          "$.metadata.tenantId"
        ],
        default: ""
      ),
      Field.string("metric_name",
        paths: ["$.metric_name", "$.name", "$.event_message"],
        default: ""
      ),
      Field.string("metric_description",
        paths: ["$.metric_description", "$.description", "$.event_message"],
        default: ""
      ),
      Field.string("metric_unit",
        paths: ["$.metric_unit", "$.unit"],
        default: ""
      ),
      Field.enum8("metric_type",
        paths: ["$.metric_type"],
        values: %{
          "gauge" => 1,
          "sum" => 2,
          "histogram" => 3,
          "exponential_histogram" => 4,
          "summary" => 5
        },
        infer: [
          %InferRule{
            result: "gauge",
            any: [%InferCondition{path: "$.gauge", predicate: "exists"}]
          },
          %InferRule{
            result: "sum",
            any: [%InferCondition{path: "$.sum", predicate: "exists"}]
          },
          %InferRule{
            result: "histogram",
            any: [%InferCondition{path: "$.histogram", predicate: "exists"}]
          },
          %InferRule{
            result: "exponential_histogram",
            any: [
              %InferCondition{path: "$.exponential_histogram", predicate: "exists"}
            ]
          },
          %InferRule{
            result: "summary",
            any: [%InferCondition{path: "$.summary", predicate: "exists"}]
          }
        ],
        default: 1
      ),
      Field.string("service_name",
        paths: [
          "$.resource.service.name",
          "$.service_name",
          "$.resource.name",
          "$.metadata.context.application"
        ],
        default: ""
      ),
      Field.string("event_message",
        paths: ["$.event_message", "$.message", "$.body", "$.msg"],
        default: ""
      ),
      Field.string("scope_name",
        paths: [
          "$.scope.name",
          "$.metadata.context.module",
          "$.metadata.context.application",
          "$.instrumentation_library.name",
          "$.metadata.namespace"
        ],
        default: ""
      ),
      Field.string("scope_version",
        paths: [
          "$.scope.version",
          "$.instrumentation_library.version"
        ],
        default: ""
      ),
      Field.string("scope_schema_url",
        paths: ["$.scope.schema_url"],
        default: ""
      ),
      Field.string("resource_schema_url",
        paths: ["$.resource.schema_url"],
        default: ""
      ),
      Field.json("resource_attributes",
        paths: ["$.resource"],
        pick: [
          {"application", ["$.metadata.context.application"]},
          {"cluster", ["$.metadata.cluster", "$.cluster", "$.resource.cluster"]},
          {"node", ["$.metadata.context.vm.node", "$.resource.node"]},
          {"project",
           [
             "$.project",
             "$.project_ref",
             "$.project_id",
             "$.metadata.project",
             "$.metadata.tenant"
           ]},
          {"region", ["$.metadata.region", "$.region"]},
          {"service_name", ["$.resource.service.name", "$.service_name"]}
        ],
        default: %{}
      ),
      Field.json("scope_attributes",
        paths: ["$.scope.attributes", "$.scope"],
        default: %{}
      ),
      Field.json("attributes",
        path: "$",
        exclude_keys: ["id", "event_message", "timestamp"],
        elevate_keys: ["metadata"]
      ),
      Field.string("aggregation_temporality",
        paths: ["$.aggregation_temporality", "$.aggregationTemporality"],
        default: ""
      ),
      Field.bool("is_monotonic",
        paths: ["$.is_monotonic", "$.isMonotonic"],
        default: false
      ),
      Field.uint32("flags",
        paths: ["$.flags"],
        default: 0
      ),
      Field.float64("value",
        paths: ["$.value", "$.gauge.value", "$.sum.value", "$.as_double", "$.as_int"],
        default: 0
      ),
      Field.uint64("count",
        paths: [
          "$.count",
          "$.histogram.count",
          "$.summary.count",
          "$.exponential_histogram.count"
        ],
        default: 0
      ),
      Field.float64("sum",
        paths: ["$.sum", "$.sum.value", "$.histogram.sum", "$.exponential_histogram.sum"],
        default: 0
      ),
      Field.float64("min",
        paths: ["$.min", "$.histogram.min", "$.exponential_histogram.min"],
        default: 0
      ),
      Field.float64("max",
        paths: ["$.max", "$.histogram.max", "$.exponential_histogram.max"],
        default: 0
      ),
      Field.int32("scale",
        paths: ["$.scale", "$.exponential_histogram.scale"],
        default: 0
      ),
      Field.uint64("zero_count",
        paths: ["$.zero_count", "$.exponential_histogram.zero_count"],
        default: 0
      ),
      Field.int32("positive_offset",
        paths: [
          "$.positive_offset",
          "$.positive.offset",
          "$.exponential_histogram.positive.offset"
        ],
        default: 0
      ),
      Field.int32("negative_offset",
        paths: [
          "$.negative_offset",
          "$.negative.offset",
          "$.exponential_histogram.negative.offset"
        ],
        default: 0
      ),
      Field.array_uint64("bucket_counts",
        path: "$.bucket_counts"
      ),
      Field.array_float64("explicit_bounds",
        path: "$.explicit_bounds"
      ),
      Field.array_uint64("positive_bucket_counts",
        paths: [
          "$.positive_bucket_counts",
          "$.positive.bucket_counts",
          "$.exponential_histogram.positive.bucket_counts"
        ]
      ),
      Field.array_uint64("negative_bucket_counts",
        paths: [
          "$.negative_bucket_counts",
          "$.negative.bucket_counts",
          "$.exponential_histogram.negative.bucket_counts"
        ]
      ),
      Field.array_float64("quantile_values",
        paths: ["$.quantile_values", "$.summary.quantile_values"]
      ),
      Field.array_float64("quantiles",
        paths: ["$.quantiles", "$.summary.quantiles"]
      ),
      Field.array_json("exemplars.filtered_attributes",
        path: "$.exemplars[*].filtered_attributes"
      ),
      Field.array_datetime64("exemplars.time_unix",
        path: "$.exemplars[*].time_unix_nano",
        precision: 9
      ),
      Field.array_float64("exemplars.value",
        paths: ["$.exemplars[*].value", "$.exemplars[*].as_double"]
      ),
      Field.array_string("exemplars.span_id",
        path: "$.exemplars[*].span_id"
      ),
      Field.array_string("exemplars.trace_id",
        path: "$.exemplars[*].trace_id"
      ),
      Field.datetime64("time_unix",
        paths: ["$.time_unix_nano", "$.timeUnixNano", "$.time_unix", "$.timestamp"],
        precision: 9
      ),
      Field.datetime64("start_time_unix",
        paths: [
          "$.start_time_unix_nano",
          "$.startTimeUnixNano",
          "$.start_time_unix",
          "$.start_time",
          "$.startTime"
        ],
        precision: 9
      ),
      Field.datetime64("timestamp", path: "$.timestamp", precision: 9)
    ])
  end

  @spec for_trace() :: MappingConfig.t()
  def for_trace do
    MappingConfig.new([
      Field.string("project",
        paths: [
          "$.project",
          "$.project_ref",
          "$.project_id",
          "$.metadata.project",
          "$.metadata.tenant",
          "$.metadata.tenantId"
        ],
        default: ""
      ),
      Field.string("trace_id",
        paths: ["$.trace_id", "$.traceId", "$.otel_trace_id"],
        default: ""
      ),
      Field.string("span_id",
        paths: ["$.span_id", "$.spanId", "$.otel_span_id"],
        default: ""
      ),
      Field.string("parent_span_id",
        paths: ["$.parent_span_id", "$.parentSpanId"],
        default: ""
      ),
      Field.string("trace_state",
        paths: ["$.trace_state", "$.traceState"],
        default: ""
      ),
      Field.string("span_name",
        paths: ["$.span_name", "$.name", "$.operationName", "$.event_message"],
        default: ""
      ),
      Field.string("span_kind",
        paths: ["$.span_kind", "$.kind", "$.spanKind"],
        default: ""
      ),
      Field.string("service_name",
        paths: [
          "$.resource.service.name",
          "$.service_name",
          "$.resource.name",
          "$.metadata.context.application"
        ],
        default: ""
      ),
      Field.string("event_message",
        paths: ["$.event_message", "$.message", "$.body", "$.msg"],
        default: ""
      ),
      Field.datetime64("start_time",
        paths: [
          "$.start_time",
          "$.startTime",
          "$.start_time_unix_nano",
          "$.startTimeUnixNano"
        ],
        precision: 9
      ),
      Field.datetime64("end_time",
        paths: [
          "$.end_time",
          "$.endTime",
          "$.end_time_unix_nano",
          "$.endTimeUnixNano"
        ],
        precision: 9
      ),
      Field.uint64("duration",
        paths: ["$.duration", "$.duration_ns", "$.duration_ms", "$.duration_us"],
        default: 0
      ),
      Field.string("status_code",
        paths: ["$.status.code", "$.status_code", "$.statusCode"],
        default: ""
      ),
      Field.string("status_message",
        paths: ["$.status.message", "$.status_message", "$.statusMessage"],
        default: ""
      ),
      Field.string("scope_name",
        paths: [
          "$.scope.name",
          "$.metadata.context.module",
          "$.metadata.context.application",
          "$.instrumentation_library.name",
          "$.metadata.namespace"
        ],
        default: ""
      ),
      Field.string("scope_version",
        paths: [
          "$.scope.version",
          "$.instrumentation_library.version"
        ],
        default: ""
      ),
      Field.json("resource_attributes",
        paths: ["$.resource"],
        pick: [
          {"application", ["$.metadata.context.application"]},
          {"cluster", ["$.metadata.cluster", "$.cluster", "$.resource.cluster"]},
          {"node", ["$.metadata.context.vm.node", "$.resource.node"]},
          {"project",
           [
             "$.project",
             "$.project_ref",
             "$.project_id",
             "$.metadata.project",
             "$.metadata.tenant"
           ]},
          {"region", ["$.metadata.region", "$.region"]},
          {"service_name", ["$.resource.service.name", "$.service_name"]}
        ],
        default: %{}
      ),
      Field.json("span_attributes",
        path: "$",
        exclude_keys: ["id", "event_message", "timestamp"],
        elevate_keys: ["metadata"]
      ),
      Field.array_datetime64("events.timestamp",
        path: "$.events[*].time_unix_nano",
        precision: 9
      ),
      Field.array_string("events.name",
        path: "$.events[*].name"
      ),
      Field.array_json("events.attributes",
        path: "$.events[*].attributes"
      ),
      Field.array_string("links.trace_id",
        path: "$.links[*].trace_id"
      ),
      Field.array_string("links.span_id",
        path: "$.links[*].span_id"
      ),
      Field.array_string("links.trace_state",
        path: "$.links[*].trace_state"
      ),
      Field.array_json("links.attributes",
        path: "$.links[*].attributes"
      ),
      Field.datetime64("timestamp", path: "$.timestamp", precision: 9)
    ])
  end

  @spec for_log_simple() :: MappingConfig.t()
  def for_log_simple, do: for_log() |> convert_json_to_flat_map()

  @spec for_metric_simple() :: MappingConfig.t()
  def for_metric_simple, do: for_metric() |> convert_json_to_flat_map()

  @spec for_trace_simple() :: MappingConfig.t()
  def for_trace_simple, do: for_trace() |> convert_json_to_flat_map()

  @spec convert_json_to_flat_map(MappingConfig.t()) :: MappingConfig.t()
  defp convert_json_to_flat_map(%MappingConfig{fields: fields} = config) do
    updated_fields =
      Enum.map(fields, fn
        %Field{type: "json"} = f -> %{f | type: "flat_map", value_type: "string"}
        %Field{type: "array_json"} = f -> %{f | type: "array_flat_map", value_type: "string"}
        f -> f
      end)

    %{config | fields: updated_fields}
  end
end
