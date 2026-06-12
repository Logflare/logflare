defmodule Event.ValueNull do
  @moduledoc false

  use Protobuf, enum: true, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :NULL_VALUE, 0
end

defmodule Event.StatisticKind do
  @moduledoc false

  use Protobuf, enum: true, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :Histogram, 0
  field :Summary, 1
end

defmodule Event.Metric.Kind do
  @moduledoc false

  use Protobuf, enum: true, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :Incremental, 0
  field :Absolute, 1
end

defmodule Event.ValueMap.FieldsEntry do
  @moduledoc false

  use Protobuf, map: true, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :key, 1, type: :string
  field :value, 2, type: Event.Value
end

defmodule Event.ValueMap do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :fields, 1, repeated: true, type: Event.ValueMap.FieldsEntry, map: true
end

defmodule Event.ValueArray do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :items, 1, repeated: true, type: Event.Value
end

defmodule Event.Value do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  oneof(:kind, 0)

  field :raw_bytes, 1, type: :bytes, json_name: "rawBytes", oneof: 0
  field :timestamp, 2, type: Google.Protobuf.Timestamp, oneof: 0
  field :integer, 4, type: :int64, oneof: 0
  field :float, 5, type: :double, oneof: 0
  field :boolean, 6, type: :bool, oneof: 0
  field :map, 7, type: Event.ValueMap, oneof: 0
  field :array, 8, type: Event.ValueArray, oneof: 0
  field :null, 9, type: Event.ValueNull, enum: true, oneof: 0
end

defmodule Event.DatadogOriginMetadata do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :origin_product, 1, proto3_optional: true, type: :uint32, json_name: "originProduct"
  field :origin_category, 2, proto3_optional: true, type: :uint32, json_name: "originCategory"
  field :origin_service, 3, proto3_optional: true, type: :uint32, json_name: "originService"
end

defmodule Event.Secrets.EntriesEntry do
  @moduledoc false

  use Protobuf, map: true, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Event.Secrets do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :entries, 1, repeated: true, type: Event.Secrets.EntriesEntry, map: true
end

defmodule Event.OutputId do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :component, 1, type: :string
  field :port, 2, proto3_optional: true, type: :string
end

defmodule Event.Metadata do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :value, 1, type: Event.Value

  field :datadog_origin_metadata, 2,
    type: Event.DatadogOriginMetadata,
    json_name: "datadogOriginMetadata"

  field :source_id, 3, proto3_optional: true, type: :string, json_name: "sourceId"
  field :source_type, 4, proto3_optional: true, type: :string, json_name: "sourceType"
  field :upstream_id, 5, type: Event.OutputId, json_name: "upstreamId"
  field :secrets, 6, type: Event.Secrets
  field :source_event_id, 7, type: :bytes, json_name: "sourceEventId"
end

defmodule Event.Log.FieldsEntry do
  @moduledoc false

  use Protobuf, map: true, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :key, 1, type: :string
  field :value, 2, type: Event.Value
end

defmodule Event.Log do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :fields, 1, repeated: true, type: Event.Log.FieldsEntry, map: true
  field :value, 2, type: Event.Value
  field :metadata, 3, type: Event.Value, deprecated: true
  field :metadata_full, 4, type: Event.Metadata, json_name: "metadataFull"
end

defmodule Event.Trace.FieldsEntry do
  @moduledoc false

  use Protobuf, map: true, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :key, 1, type: :string
  field :value, 2, type: Event.Value
end

defmodule Event.Trace do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :fields, 1, repeated: true, type: Event.Trace.FieldsEntry, map: true
  field :metadata, 2, type: Event.Value, deprecated: true
  field :metadata_full, 3, type: Event.Metadata, json_name: "metadataFull"
end

defmodule Event.TagValue do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :value, 1, proto3_optional: true, type: :string
end

defmodule Event.TagValues do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :values, 1, repeated: true, type: Event.TagValue
end

defmodule Event.Counter do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :value, 1, type: :double
end

defmodule Event.Gauge do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :value, 1, type: :double
end

defmodule Event.Set do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :values, 1, repeated: true, type: :string
end

defmodule Event.Distribution1 do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :values, 1, repeated: true, type: :double
  field :sample_rates, 2, repeated: true, type: :uint32, json_name: "sampleRates"
  field :statistic, 3, type: Event.StatisticKind, enum: true
end

defmodule Event.DistributionSample do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :value, 1, type: :double
  field :rate, 2, type: :uint32
end

defmodule Event.Distribution2 do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :samples, 1, repeated: true, type: Event.DistributionSample
  field :statistic, 2, type: Event.StatisticKind, enum: true
end

defmodule Event.HistogramBucket do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :upper_limit, 1, type: :double, json_name: "upperLimit"
  field :count, 2, type: :uint32
end

defmodule Event.HistogramBucket3 do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :upper_limit, 1, type: :double, json_name: "upperLimit"
  field :count, 2, type: :uint64
end

defmodule Event.AggregatedHistogram1 do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :buckets, 1, repeated: true, type: :double
  field :counts, 2, repeated: true, type: :uint32
  field :count, 3, type: :uint32
  field :sum, 4, type: :double
end

defmodule Event.AggregatedHistogram2 do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :buckets, 1, repeated: true, type: Event.HistogramBucket
  field :count, 2, type: :uint32
  field :sum, 3, type: :double
end

defmodule Event.AggregatedHistogram3 do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :buckets, 1, repeated: true, type: Event.HistogramBucket3
  field :count, 2, type: :uint64
  field :sum, 3, type: :double
end

defmodule Event.SummaryQuantile do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :quantile, 1, type: :double
  field :value, 2, type: :double
end

defmodule Event.AggregatedSummary1 do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :quantiles, 1, repeated: true, type: :double
  field :values, 2, repeated: true, type: :double
  field :count, 3, type: :uint32
  field :sum, 4, type: :double
end

defmodule Event.AggregatedSummary2 do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :quantiles, 1, repeated: true, type: Event.SummaryQuantile
  field :count, 2, type: :uint32
  field :sum, 3, type: :double
end

defmodule Event.AggregatedSummary3 do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :quantiles, 1, repeated: true, type: Event.SummaryQuantile
  field :count, 2, type: :uint64
  field :sum, 3, type: :double
end

defmodule Event.Sketch.AgentDDSketch do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :count, 1, type: :uint32
  field :min, 2, type: :double
  field :max, 3, type: :double
  field :sum, 4, type: :double
  field :avg, 5, type: :double
  field :k, 6, repeated: true, type: :sint32
  field :n, 7, repeated: true, type: :uint32
end

defmodule Event.Sketch do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  oneof(:sketch, 0)

  field :agent_dd_sketch, 1,
    type: Event.Sketch.AgentDDSketch,
    json_name: "agentDdSketch",
    oneof: 0
end

defmodule Event.Metric.TagsV1Entry do
  @moduledoc false

  use Protobuf, map: true, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Event.Metric.TagsV2Entry do
  @moduledoc false

  use Protobuf, map: true, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :key, 1, type: :string
  field :value, 2, type: Event.TagValues
end

defmodule Event.Metric do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  oneof(:value, 0)

  field :name, 1, type: :string
  field :timestamp, 2, type: Google.Protobuf.Timestamp
  field :tags_v1, 3, repeated: true, type: Event.Metric.TagsV1Entry, json_name: "tagsV1", map: true

  field :tags_v2, 20,
    repeated: true,
    type: Event.Metric.TagsV2Entry,
    json_name: "tagsV2",
    map: true

  field :kind, 4, type: Event.Metric.Kind, enum: true
  field :counter, 5, type: Event.Counter, oneof: 0
  field :gauge, 6, type: Event.Gauge, oneof: 0
  field :set, 7, type: Event.Set, oneof: 0
  field :distribution1, 8, type: Event.Distribution1, oneof: 0
  field :aggregated_histogram1, 9, type: Event.AggregatedHistogram1, oneof: 0
  field :aggregated_summary1, 10, type: Event.AggregatedSummary1, oneof: 0
  field :distribution2, 12, type: Event.Distribution2, oneof: 0
  field :aggregated_histogram2, 13, type: Event.AggregatedHistogram2, oneof: 0
  field :aggregated_summary2, 14, type: Event.AggregatedSummary2, oneof: 0
  field :sketch, 15, type: Event.Sketch, oneof: 0
  field :aggregated_histogram3, 16, type: Event.AggregatedHistogram3, oneof: 0
  field :aggregated_summary3, 17, type: Event.AggregatedSummary3, oneof: 0
  field :namespace, 11, type: :string
  field :interval_ms, 18, type: :uint32, json_name: "intervalMs"
  field :metadata, 19, type: Event.Value, deprecated: true
  field :metadata_full, 21, type: Event.Metadata, json_name: "metadataFull"
end

defmodule Event.LogArray do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :logs, 1, repeated: true, type: Event.Log
end

defmodule Event.MetricArray do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :metrics, 1, repeated: true, type: Event.Metric
end

defmodule Event.TraceArray do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :traces, 1, repeated: true, type: Event.Trace
end

defmodule Event.EventArray do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  oneof(:events, 0)

  field :logs, 1, type: Event.LogArray, oneof: 0
  field :metrics, 2, type: Event.MetricArray, oneof: 0
  field :traces, 3, type: Event.TraceArray, oneof: 0
end

defmodule Event.EventWrapper do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  oneof(:event, 0)

  field :log, 1, type: Event.Log, oneof: 0
  field :metric, 2, type: Event.Metric, oneof: 0
  field :trace, 3, type: Event.Trace, oneof: 0
end
