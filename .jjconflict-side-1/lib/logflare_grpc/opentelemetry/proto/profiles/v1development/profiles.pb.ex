defmodule Opentelemetry.Proto.Profiles.V1development.AggregationTemporality do
  @moduledoc false

  use Protobuf, enum: true, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :AGGREGATION_TEMPORALITY_UNSPECIFIED, 0
  field :AGGREGATION_TEMPORALITY_DELTA, 1
  field :AGGREGATION_TEMPORALITY_CUMULATIVE, 2
end

defmodule Opentelemetry.Proto.Profiles.V1development.ProfilesDictionary do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :mapping_table, 1,
    repeated: true,
    type: Opentelemetry.Proto.Profiles.V1development.Mapping,
    json_name: "mappingTable"

  field :location_table, 2,
    repeated: true,
    type: Opentelemetry.Proto.Profiles.V1development.Location,
    json_name: "locationTable"

  field :function_table, 3,
    repeated: true,
    type: Opentelemetry.Proto.Profiles.V1development.Function,
    json_name: "functionTable"

  field :link_table, 4,
    repeated: true,
    type: Opentelemetry.Proto.Profiles.V1development.Link,
    json_name: "linkTable"

  field :string_table, 5, repeated: true, type: :string, json_name: "stringTable"

  field :attribute_table, 6,
    repeated: true,
    type: Opentelemetry.Proto.Profiles.V1development.KeyValueAndUnit,
    json_name: "attributeTable"

  field :stack_table, 7,
    repeated: true,
    type: Opentelemetry.Proto.Profiles.V1development.Stack,
    json_name: "stackTable"
end

defmodule Opentelemetry.Proto.Profiles.V1development.ProfilesData do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :resource_profiles, 1,
    repeated: true,
    type: Opentelemetry.Proto.Profiles.V1development.ResourceProfiles,
    json_name: "resourceProfiles"

  field :dictionary, 2, type: Opentelemetry.Proto.Profiles.V1development.ProfilesDictionary
end

defmodule Opentelemetry.Proto.Profiles.V1development.ResourceProfiles do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :resource, 1, type: Opentelemetry.Proto.Resource.V1.Resource

  field :scope_profiles, 2,
    repeated: true,
    type: Opentelemetry.Proto.Profiles.V1development.ScopeProfiles,
    json_name: "scopeProfiles"

  field :schema_url, 3, type: :string, json_name: "schemaUrl"
end

defmodule Opentelemetry.Proto.Profiles.V1development.ScopeProfiles do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :scope, 1, type: Opentelemetry.Proto.Common.V1.InstrumentationScope
  field :profiles, 2, repeated: true, type: Opentelemetry.Proto.Profiles.V1development.Profile
  field :schema_url, 3, type: :string, json_name: "schemaUrl"
end

defmodule Opentelemetry.Proto.Profiles.V1development.Profile do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :sample_type, 1,
    type: Opentelemetry.Proto.Profiles.V1development.ValueType,
    json_name: "sampleType"

  field :sample, 2, repeated: true, type: Opentelemetry.Proto.Profiles.V1development.Sample
  field :time_unix_nano, 3, type: :fixed64, json_name: "timeUnixNano"
  field :duration_nano, 4, type: :uint64, json_name: "durationNano"

  field :period_type, 5,
    type: Opentelemetry.Proto.Profiles.V1development.ValueType,
    json_name: "periodType"

  field :period, 6, type: :int64
  field :comment_strindices, 7, repeated: true, type: :int32, json_name: "commentStrindices"
  field :profile_id, 8, type: :bytes, json_name: "profileId"
  field :dropped_attributes_count, 9, type: :uint32, json_name: "droppedAttributesCount"
  field :original_payload_format, 10, type: :string, json_name: "originalPayloadFormat"
  field :original_payload, 11, type: :bytes, json_name: "originalPayload"
  field :attribute_indices, 12, repeated: true, type: :int32, json_name: "attributeIndices"
end

defmodule Opentelemetry.Proto.Profiles.V1development.Link do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :trace_id, 1, type: :bytes, json_name: "traceId"
  field :span_id, 2, type: :bytes, json_name: "spanId"
end

defmodule Opentelemetry.Proto.Profiles.V1development.ValueType do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :type_strindex, 1, type: :int32, json_name: "typeStrindex"
  field :unit_strindex, 2, type: :int32, json_name: "unitStrindex"

  field :aggregation_temporality, 3,
    type: Opentelemetry.Proto.Profiles.V1development.AggregationTemporality,
    json_name: "aggregationTemporality",
    enum: true
end

defmodule Opentelemetry.Proto.Profiles.V1development.Sample do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :stack_index, 1, type: :int32, json_name: "stackIndex"
  field :values, 2, repeated: true, type: :int64
  field :attribute_indices, 3, repeated: true, type: :int32, json_name: "attributeIndices"
  field :link_index, 4, type: :int32, json_name: "linkIndex"
  field :timestamps_unix_nano, 5, repeated: true, type: :fixed64, json_name: "timestampsUnixNano"
end

defmodule Opentelemetry.Proto.Profiles.V1development.Mapping do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :memory_start, 1, type: :uint64, json_name: "memoryStart"
  field :memory_limit, 2, type: :uint64, json_name: "memoryLimit"
  field :file_offset, 3, type: :uint64, json_name: "fileOffset"
  field :filename_strindex, 4, type: :int32, json_name: "filenameStrindex"
  field :attribute_indices, 5, repeated: true, type: :int32, json_name: "attributeIndices"
end

defmodule Opentelemetry.Proto.Profiles.V1development.Stack do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :location_indices, 1, repeated: true, type: :int32, json_name: "locationIndices"
end

defmodule Opentelemetry.Proto.Profiles.V1development.Location do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :mapping_index, 1, type: :int32, json_name: "mappingIndex"
  field :address, 2, type: :uint64
  field :line, 3, repeated: true, type: Opentelemetry.Proto.Profiles.V1development.Line
  field :attribute_indices, 4, repeated: true, type: :int32, json_name: "attributeIndices"
end

defmodule Opentelemetry.Proto.Profiles.V1development.Line do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :function_index, 1, type: :int32, json_name: "functionIndex"
  field :line, 2, type: :int64
  field :column, 3, type: :int64
end

defmodule Opentelemetry.Proto.Profiles.V1development.Function do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :name_strindex, 1, type: :int32, json_name: "nameStrindex"
  field :system_name_strindex, 2, type: :int32, json_name: "systemNameStrindex"
  field :filename_strindex, 3, type: :int32, json_name: "filenameStrindex"
  field :start_line, 4, type: :int64, json_name: "startLine"
end

defmodule Opentelemetry.Proto.Profiles.V1development.KeyValueAndUnit do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :key_strindex, 1, type: :int32, json_name: "keyStrindex"
  field :value, 2, type: Opentelemetry.Proto.Common.V1.AnyValue
  field :unit_strindex, 3, type: :int32, json_name: "unitStrindex"
end
