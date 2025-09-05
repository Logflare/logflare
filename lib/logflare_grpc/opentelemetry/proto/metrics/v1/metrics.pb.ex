defmodule Opentelemetry.Proto.Metrics.V1.AggregationTemporality do
  @moduledoc false

  use Protobuf, enum: true, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :AGGREGATION_TEMPORALITY_UNSPECIFIED, 0
  field :AGGREGATION_TEMPORALITY_DELTA, 1
  field :AGGREGATION_TEMPORALITY_CUMULATIVE, 2
end

defmodule Opentelemetry.Proto.Metrics.V1.DataPointFlags do
  @moduledoc false

  use Protobuf, enum: true, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :DATA_POINT_FLAGS_DO_NOT_USE, 0
  field :DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK, 1
end

defmodule Opentelemetry.Proto.Metrics.V1.MetricsData do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :resource_metrics, 1,
    repeated: true,
    type: Opentelemetry.Proto.Metrics.V1.ResourceMetrics,
    json_name: "resourceMetrics"
end

defmodule Opentelemetry.Proto.Metrics.V1.ResourceMetrics do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :resource, 1, type: Opentelemetry.Proto.Resource.V1.Resource

  field :scope_metrics, 2,
    repeated: true,
    type: Opentelemetry.Proto.Metrics.V1.ScopeMetrics,
    json_name: "scopeMetrics"

  field :schema_url, 3, type: :string, json_name: "schemaUrl"
end

defmodule Opentelemetry.Proto.Metrics.V1.ScopeMetrics do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :scope, 1, type: Opentelemetry.Proto.Common.V1.InstrumentationScope
  field :metrics, 2, repeated: true, type: Opentelemetry.Proto.Metrics.V1.Metric
  field :schema_url, 3, type: :string, json_name: "schemaUrl"
end

defmodule Opentelemetry.Proto.Metrics.V1.Metric do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  oneof(:data, 0)

  field :name, 1, type: :string
  field :description, 2, type: :string
  field :unit, 3, type: :string
  field :gauge, 5, type: Opentelemetry.Proto.Metrics.V1.Gauge, oneof: 0
  field :sum, 7, type: Opentelemetry.Proto.Metrics.V1.Sum, oneof: 0
  field :histogram, 9, type: Opentelemetry.Proto.Metrics.V1.Histogram, oneof: 0

  field :exponential_histogram, 10,
    type: Opentelemetry.Proto.Metrics.V1.ExponentialHistogram,
    json_name: "exponentialHistogram",
    oneof: 0

  field :summary, 11, type: Opentelemetry.Proto.Metrics.V1.Summary, oneof: 0
  field :metadata, 12, repeated: true, type: Opentelemetry.Proto.Common.V1.KeyValue
end

defmodule Opentelemetry.Proto.Metrics.V1.Gauge do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :data_points, 1,
    repeated: true,
    type: Opentelemetry.Proto.Metrics.V1.NumberDataPoint,
    json_name: "dataPoints"
end

defmodule Opentelemetry.Proto.Metrics.V1.Sum do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :data_points, 1,
    repeated: true,
    type: Opentelemetry.Proto.Metrics.V1.NumberDataPoint,
    json_name: "dataPoints"

  field :aggregation_temporality, 2,
    type: Opentelemetry.Proto.Metrics.V1.AggregationTemporality,
    json_name: "aggregationTemporality",
    enum: true

  field :is_monotonic, 3, type: :bool, json_name: "isMonotonic"
end

defmodule Opentelemetry.Proto.Metrics.V1.Histogram do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :data_points, 1,
    repeated: true,
    type: Opentelemetry.Proto.Metrics.V1.HistogramDataPoint,
    json_name: "dataPoints"

  field :aggregation_temporality, 2,
    type: Opentelemetry.Proto.Metrics.V1.AggregationTemporality,
    json_name: "aggregationTemporality",
    enum: true
end

defmodule Opentelemetry.Proto.Metrics.V1.ExponentialHistogram do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :data_points, 1,
    repeated: true,
    type: Opentelemetry.Proto.Metrics.V1.ExponentialHistogramDataPoint,
    json_name: "dataPoints"

  field :aggregation_temporality, 2,
    type: Opentelemetry.Proto.Metrics.V1.AggregationTemporality,
    json_name: "aggregationTemporality",
    enum: true
end

defmodule Opentelemetry.Proto.Metrics.V1.Summary do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :data_points, 1,
    repeated: true,
    type: Opentelemetry.Proto.Metrics.V1.SummaryDataPoint,
    json_name: "dataPoints"
end

defmodule Opentelemetry.Proto.Metrics.V1.NumberDataPoint do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  oneof(:value, 0)

  field :attributes, 7, repeated: true, type: Opentelemetry.Proto.Common.V1.KeyValue
  field :start_time_unix_nano, 2, type: :fixed64, json_name: "startTimeUnixNano"
  field :time_unix_nano, 3, type: :fixed64, json_name: "timeUnixNano"
  field :as_double, 4, type: :double, json_name: "asDouble", oneof: 0
  field :as_int, 6, type: :sfixed64, json_name: "asInt", oneof: 0
  field :exemplars, 5, repeated: true, type: Opentelemetry.Proto.Metrics.V1.Exemplar
  field :flags, 8, type: :uint32
end

defmodule Opentelemetry.Proto.Metrics.V1.HistogramDataPoint do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :attributes, 9, repeated: true, type: Opentelemetry.Proto.Common.V1.KeyValue
  field :start_time_unix_nano, 2, type: :fixed64, json_name: "startTimeUnixNano"
  field :time_unix_nano, 3, type: :fixed64, json_name: "timeUnixNano"
  field :count, 4, type: :fixed64
  field :sum, 5, proto3_optional: true, type: :double
  field :bucket_counts, 6, repeated: true, type: :fixed64, json_name: "bucketCounts"
  field :explicit_bounds, 7, repeated: true, type: :double, json_name: "explicitBounds"
  field :exemplars, 8, repeated: true, type: Opentelemetry.Proto.Metrics.V1.Exemplar
  field :flags, 10, type: :uint32
  field :min, 11, proto3_optional: true, type: :double
  field :max, 12, proto3_optional: true, type: :double
end

defmodule Opentelemetry.Proto.Metrics.V1.ExponentialHistogramDataPoint.Buckets do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :offset, 1, type: :sint32
  field :bucket_counts, 2, repeated: true, type: :uint64, json_name: "bucketCounts"
end

defmodule Opentelemetry.Proto.Metrics.V1.ExponentialHistogramDataPoint do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :attributes, 1, repeated: true, type: Opentelemetry.Proto.Common.V1.KeyValue
  field :start_time_unix_nano, 2, type: :fixed64, json_name: "startTimeUnixNano"
  field :time_unix_nano, 3, type: :fixed64, json_name: "timeUnixNano"
  field :count, 4, type: :fixed64
  field :sum, 5, proto3_optional: true, type: :double
  field :scale, 6, type: :sint32
  field :zero_count, 7, type: :fixed64, json_name: "zeroCount"
  field :positive, 8, type: Opentelemetry.Proto.Metrics.V1.ExponentialHistogramDataPoint.Buckets
  field :negative, 9, type: Opentelemetry.Proto.Metrics.V1.ExponentialHistogramDataPoint.Buckets
  field :flags, 10, type: :uint32
  field :exemplars, 11, repeated: true, type: Opentelemetry.Proto.Metrics.V1.Exemplar
  field :min, 12, proto3_optional: true, type: :double
  field :max, 13, proto3_optional: true, type: :double
  field :zero_threshold, 14, type: :double, json_name: "zeroThreshold"
end

defmodule Opentelemetry.Proto.Metrics.V1.SummaryDataPoint.ValueAtQuantile do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :quantile, 1, type: :double
  field :value, 2, type: :double
end

defmodule Opentelemetry.Proto.Metrics.V1.SummaryDataPoint do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :attributes, 7, repeated: true, type: Opentelemetry.Proto.Common.V1.KeyValue
  field :start_time_unix_nano, 2, type: :fixed64, json_name: "startTimeUnixNano"
  field :time_unix_nano, 3, type: :fixed64, json_name: "timeUnixNano"
  field :count, 4, type: :fixed64
  field :sum, 5, type: :double

  field :quantile_values, 6,
    repeated: true,
    type: Opentelemetry.Proto.Metrics.V1.SummaryDataPoint.ValueAtQuantile,
    json_name: "quantileValues"

  field :flags, 8, type: :uint32
end

defmodule Opentelemetry.Proto.Metrics.V1.Exemplar do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  oneof(:value, 0)

  field :filtered_attributes, 7,
    repeated: true,
    type: Opentelemetry.Proto.Common.V1.KeyValue,
    json_name: "filteredAttributes"

  field :time_unix_nano, 2, type: :fixed64, json_name: "timeUnixNano"
  field :as_double, 3, type: :double, json_name: "asDouble", oneof: 0
  field :as_int, 6, type: :sfixed64, json_name: "asInt", oneof: 0
  field :span_id, 4, type: :bytes, json_name: "spanId"
  field :trace_id, 5, type: :bytes, json_name: "traceId"
end
