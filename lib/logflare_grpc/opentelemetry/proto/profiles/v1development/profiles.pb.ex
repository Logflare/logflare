defmodule Opentelemetry.Proto.Profiles.V1development.AggregationTemporality do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :AGGREGATION_TEMPORALITY_UNSPECIFIED, 0
  field :AGGREGATION_TEMPORALITY_DELTA, 1
  field :AGGREGATION_TEMPORALITY_CUMULATIVE, 2
end

defmodule Opentelemetry.Proto.Profiles.V1development.ProfilesData do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :resource_profiles, 1,
    repeated: true,
    type: Opentelemetry.Proto.Profiles.V1development.ResourceProfiles,
    json_name: "resourceProfiles"
end

defmodule Opentelemetry.Proto.Profiles.V1development.ResourceProfiles do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :resource, 1, type: Opentelemetry.Proto.Resource.V1.Resource

  field :scope_profiles, 2,
    repeated: true,
    type: Opentelemetry.Proto.Profiles.V1development.ScopeProfiles,
    json_name: "scopeProfiles"

  field :schema_url, 3, type: :string, json_name: "schemaUrl"
end

defmodule Opentelemetry.Proto.Profiles.V1development.ScopeProfiles do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :scope, 1, type: Opentelemetry.Proto.Common.V1.InstrumentationScope
  field :profiles, 2, repeated: true, type: Opentelemetry.Proto.Profiles.V1development.Profile
  field :schema_url, 3, type: :string, json_name: "schemaUrl"
end

defmodule Opentelemetry.Proto.Profiles.V1development.Profile do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :sample_type, 1,
    repeated: true,
    type: Opentelemetry.Proto.Profiles.V1development.ValueType,
    json_name: "sampleType"

  field :sample, 2, repeated: true, type: Opentelemetry.Proto.Profiles.V1development.Sample

  field :mapping_table, 3,
    repeated: true,
    type: Opentelemetry.Proto.Profiles.V1development.Mapping,
    json_name: "mappingTable"

  field :location_table, 4,
    repeated: true,
    type: Opentelemetry.Proto.Profiles.V1development.Location,
    json_name: "locationTable"

  field :location_indices, 5, repeated: true, type: :int32, json_name: "locationIndices"

  field :function_table, 6,
    repeated: true,
    type: Opentelemetry.Proto.Profiles.V1development.Function,
    json_name: "functionTable"

  field :attribute_table, 7,
    repeated: true,
    type: Opentelemetry.Proto.Common.V1.KeyValue,
    json_name: "attributeTable"

  field :attribute_units, 8,
    repeated: true,
    type: Opentelemetry.Proto.Profiles.V1development.AttributeUnit,
    json_name: "attributeUnits"

  field :link_table, 9,
    repeated: true,
    type: Opentelemetry.Proto.Profiles.V1development.Link,
    json_name: "linkTable"

  field :string_table, 10, repeated: true, type: :string, json_name: "stringTable"
  field :time_nanos, 11, type: :int64, json_name: "timeNanos"
  field :duration_nanos, 12, type: :int64, json_name: "durationNanos"

  field :period_type, 13,
    type: Opentelemetry.Proto.Profiles.V1development.ValueType,
    json_name: "periodType"

  field :period, 14, type: :int64
  field :comment_strindices, 15, repeated: true, type: :int32, json_name: "commentStrindices"
  field :default_sample_type_strindex, 16, type: :int32, json_name: "defaultSampleTypeStrindex"
  field :profile_id, 17, type: :bytes, json_name: "profileId"
  field :dropped_attributes_count, 19, type: :uint32, json_name: "droppedAttributesCount"
  field :original_payload_format, 20, type: :string, json_name: "originalPayloadFormat"
  field :original_payload, 21, type: :bytes, json_name: "originalPayload"
  field :attribute_indices, 22, repeated: true, type: :int32, json_name: "attributeIndices"
end

defmodule Opentelemetry.Proto.Profiles.V1development.AttributeUnit do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :attribute_key_strindex, 1, type: :int32, json_name: "attributeKeyStrindex"
  field :unit_strindex, 2, type: :int32, json_name: "unitStrindex"
end

defmodule Opentelemetry.Proto.Profiles.V1development.Link do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :trace_id, 1, type: :bytes, json_name: "traceId"
  field :span_id, 2, type: :bytes, json_name: "spanId"
end

defmodule Opentelemetry.Proto.Profiles.V1development.ValueType do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :type_strindex, 1, type: :int32, json_name: "typeStrindex"
  field :unit_strindex, 2, type: :int32, json_name: "unitStrindex"

  field :aggregation_temporality, 3,
    type: Opentelemetry.Proto.Profiles.V1development.AggregationTemporality,
    json_name: "aggregationTemporality",
    enum: true
end

defmodule Opentelemetry.Proto.Profiles.V1development.Sample do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :locations_start_index, 1, type: :int32, json_name: "locationsStartIndex"
  field :locations_length, 2, type: :int32, json_name: "locationsLength"
  field :value, 3, repeated: true, type: :int64
  field :attribute_indices, 4, repeated: true, type: :int32, json_name: "attributeIndices"
  field :link_index, 5, proto3_optional: true, type: :int32, json_name: "linkIndex"
  field :timestamps_unix_nano, 6, repeated: true, type: :uint64, json_name: "timestampsUnixNano"
end

defmodule Opentelemetry.Proto.Profiles.V1development.Mapping do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :memory_start, 1, type: :uint64, json_name: "memoryStart"
  field :memory_limit, 2, type: :uint64, json_name: "memoryLimit"
  field :file_offset, 3, type: :uint64, json_name: "fileOffset"
  field :filename_strindex, 4, type: :int32, json_name: "filenameStrindex"
  field :attribute_indices, 5, repeated: true, type: :int32, json_name: "attributeIndices"
  field :has_functions, 6, type: :bool, json_name: "hasFunctions"
  field :has_filenames, 7, type: :bool, json_name: "hasFilenames"
  field :has_line_numbers, 8, type: :bool, json_name: "hasLineNumbers"
  field :has_inline_frames, 9, type: :bool, json_name: "hasInlineFrames"
end

defmodule Opentelemetry.Proto.Profiles.V1development.Location do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :mapping_index, 1, proto3_optional: true, type: :int32, json_name: "mappingIndex"
  field :address, 2, type: :uint64
  field :line, 3, repeated: true, type: Opentelemetry.Proto.Profiles.V1development.Line
  field :is_folded, 4, type: :bool, json_name: "isFolded"
  field :attribute_indices, 5, repeated: true, type: :int32, json_name: "attributeIndices"
end

defmodule Opentelemetry.Proto.Profiles.V1development.Line do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :function_index, 1, type: :int32, json_name: "functionIndex"
  field :line, 2, type: :int64
  field :column, 3, type: :int64
end

defmodule Opentelemetry.Proto.Profiles.V1development.Function do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name_strindex, 1, type: :int32, json_name: "nameStrindex"
  field :system_name_strindex, 2, type: :int32, json_name: "systemNameStrindex"
  field :filename_strindex, 3, type: :int32, json_name: "filenameStrindex"
  field :start_line, 4, type: :int64, json_name: "startLine"
end
