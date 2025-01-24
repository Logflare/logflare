defmodule Opentelemetry.Proto.Profiles.V1development.AggregationTemporality do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.EnumDescriptorProto{
      name: "AggregationTemporality",
      value: [
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "AGGREGATION_TEMPORALITY_UNSPECIFIED",
          number: 0,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "AGGREGATION_TEMPORALITY_DELTA",
          number: 1,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "AGGREGATION_TEMPORALITY_CUMULATIVE",
          number: 2,
          options: nil,
          __unknown_fields__: []
        }
      ],
      options: nil,
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field :AGGREGATION_TEMPORALITY_UNSPECIFIED, 0
  field :AGGREGATION_TEMPORALITY_DELTA, 1
  field :AGGREGATION_TEMPORALITY_CUMULATIVE, 2
end

defmodule Opentelemetry.Proto.Profiles.V1development.ProfilesData do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "ProfilesData",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "resource_profiles",
          extendee: nil,
          number: 1,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.profiles.v1development.ResourceProfiles",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "resourceProfiles",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field :resource_profiles, 1,
    repeated: true,
    type: Opentelemetry.Proto.Profiles.V1development.ResourceProfiles,
    json_name: "resourceProfiles"
end

defmodule Opentelemetry.Proto.Profiles.V1development.ResourceProfiles do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "ResourceProfiles",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "resource",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.resource.v1.Resource",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "resource",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "scope_profiles",
          extendee: nil,
          number: 2,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.profiles.v1development.ScopeProfiles",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "scopeProfiles",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "schema_url",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "schemaUrl",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [],
      reserved_range: [
        %Google.Protobuf.DescriptorProto.ReservedRange{
          start: 1000,
          end: 1001,
          __unknown_fields__: []
        }
      ],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

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

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "ScopeProfiles",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "scope",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.common.v1.InstrumentationScope",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "scope",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "profiles",
          extendee: nil,
          number: 2,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.profiles.v1development.Profile",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "profiles",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "schema_url",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "schemaUrl",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field :scope, 1, type: Opentelemetry.Proto.Common.V1.InstrumentationScope
  field :profiles, 2, repeated: true, type: Opentelemetry.Proto.Profiles.V1development.Profile
  field :schema_url, 3, type: :string, json_name: "schemaUrl"
end

defmodule Opentelemetry.Proto.Profiles.V1development.Profile do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "Profile",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "sample_type",
          extendee: nil,
          number: 1,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.profiles.v1development.ValueType",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "sampleType",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "sample",
          extendee: nil,
          number: 2,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.profiles.v1development.Sample",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "sample",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "mapping_table",
          extendee: nil,
          number: 3,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.profiles.v1development.Mapping",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "mappingTable",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "location_table",
          extendee: nil,
          number: 4,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.profiles.v1development.Location",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "locationTable",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "location_indices",
          extendee: nil,
          number: 5,
          label: :LABEL_REPEATED,
          type: :TYPE_INT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "locationIndices",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "function_table",
          extendee: nil,
          number: 6,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.profiles.v1development.Function",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "functionTable",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "attribute_table",
          extendee: nil,
          number: 7,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.common.v1.KeyValue",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "attributeTable",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "attribute_units",
          extendee: nil,
          number: 8,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.profiles.v1development.AttributeUnit",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "attributeUnits",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "link_table",
          extendee: nil,
          number: 9,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.profiles.v1development.Link",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "linkTable",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "string_table",
          extendee: nil,
          number: 10,
          label: :LABEL_REPEATED,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "stringTable",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "time_nanos",
          extendee: nil,
          number: 11,
          label: :LABEL_OPTIONAL,
          type: :TYPE_INT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "timeNanos",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "duration_nanos",
          extendee: nil,
          number: 12,
          label: :LABEL_OPTIONAL,
          type: :TYPE_INT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "durationNanos",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "period_type",
          extendee: nil,
          number: 13,
          label: :LABEL_OPTIONAL,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.profiles.v1development.ValueType",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "periodType",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "period",
          extendee: nil,
          number: 14,
          label: :LABEL_OPTIONAL,
          type: :TYPE_INT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "period",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "comment_strindices",
          extendee: nil,
          number: 15,
          label: :LABEL_REPEATED,
          type: :TYPE_INT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "commentStrindices",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "default_sample_type_strindex",
          extendee: nil,
          number: 16,
          label: :LABEL_OPTIONAL,
          type: :TYPE_INT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "defaultSampleTypeStrindex",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "profile_id",
          extendee: nil,
          number: 17,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BYTES,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "profileId",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "dropped_attributes_count",
          extendee: nil,
          number: 19,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "droppedAttributesCount",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "original_payload_format",
          extendee: nil,
          number: 20,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "originalPayloadFormat",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "original_payload",
          extendee: nil,
          number: 21,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BYTES,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "originalPayload",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "attribute_indices",
          extendee: nil,
          number: 22,
          label: :LABEL_REPEATED,
          type: :TYPE_INT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "attributeIndices",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

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

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "AttributeUnit",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "attribute_key_strindex",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_INT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "attributeKeyStrindex",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "unit_strindex",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_INT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "unitStrindex",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field :attribute_key_strindex, 1, type: :int32, json_name: "attributeKeyStrindex"
  field :unit_strindex, 2, type: :int32, json_name: "unitStrindex"
end

defmodule Opentelemetry.Proto.Profiles.V1development.Link do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "Link",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "trace_id",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BYTES,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "traceId",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "span_id",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BYTES,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "spanId",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field :trace_id, 1, type: :bytes, json_name: "traceId"
  field :span_id, 2, type: :bytes, json_name: "spanId"
end

defmodule Opentelemetry.Proto.Profiles.V1development.ValueType do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "ValueType",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "type_strindex",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_INT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "typeStrindex",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "unit_strindex",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_INT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "unitStrindex",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "aggregation_temporality",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_ENUM,
          type_name: ".opentelemetry.proto.profiles.v1development.AggregationTemporality",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "aggregationTemporality",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

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

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "Sample",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "locations_start_index",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_INT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "locationsStartIndex",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "locations_length",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_INT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "locationsLength",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "value",
          extendee: nil,
          number: 3,
          label: :LABEL_REPEATED,
          type: :TYPE_INT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "value",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "attribute_indices",
          extendee: nil,
          number: 4,
          label: :LABEL_REPEATED,
          type: :TYPE_INT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "attributeIndices",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "link_index",
          extendee: nil,
          number: 5,
          label: :LABEL_OPTIONAL,
          type: :TYPE_INT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: 0,
          json_name: "linkIndex",
          proto3_optional: true,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "timestamps_unix_nano",
          extendee: nil,
          number: 6,
          label: :LABEL_REPEATED,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "timestampsUnixNano",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [
        %Google.Protobuf.OneofDescriptorProto{
          name: "_link_index",
          options: nil,
          __unknown_fields__: []
        }
      ],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

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

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "Mapping",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "memory_start",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "memoryStart",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "memory_limit",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "memoryLimit",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "file_offset",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "fileOffset",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "filename_strindex",
          extendee: nil,
          number: 4,
          label: :LABEL_OPTIONAL,
          type: :TYPE_INT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "filenameStrindex",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "attribute_indices",
          extendee: nil,
          number: 5,
          label: :LABEL_REPEATED,
          type: :TYPE_INT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "attributeIndices",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "has_functions",
          extendee: nil,
          number: 6,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BOOL,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "hasFunctions",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "has_filenames",
          extendee: nil,
          number: 7,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BOOL,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "hasFilenames",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "has_line_numbers",
          extendee: nil,
          number: 8,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BOOL,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "hasLineNumbers",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "has_inline_frames",
          extendee: nil,
          number: 9,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BOOL,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "hasInlineFrames",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

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

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "Location",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "mapping_index",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_INT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: 0,
          json_name: "mappingIndex",
          proto3_optional: true,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "address",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "address",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "line",
          extendee: nil,
          number: 3,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.profiles.v1development.Line",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "line",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "is_folded",
          extendee: nil,
          number: 4,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BOOL,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "isFolded",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "attribute_indices",
          extendee: nil,
          number: 5,
          label: :LABEL_REPEATED,
          type: :TYPE_INT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "attributeIndices",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [
        %Google.Protobuf.OneofDescriptorProto{
          name: "_mapping_index",
          options: nil,
          __unknown_fields__: []
        }
      ],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field :mapping_index, 1, proto3_optional: true, type: :int32, json_name: "mappingIndex"
  field :address, 2, type: :uint64
  field :line, 3, repeated: true, type: Opentelemetry.Proto.Profiles.V1development.Line
  field :is_folded, 4, type: :bool, json_name: "isFolded"
  field :attribute_indices, 5, repeated: true, type: :int32, json_name: "attributeIndices"
end

defmodule Opentelemetry.Proto.Profiles.V1development.Line do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "Line",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "function_index",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_INT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "functionIndex",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "line",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_INT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "line",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "column",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_INT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "column",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field :function_index, 1, type: :int32, json_name: "functionIndex"
  field :line, 2, type: :int64
  field :column, 3, type: :int64
end

defmodule Opentelemetry.Proto.Profiles.V1development.Function do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "Function",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "name_strindex",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_INT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "nameStrindex",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "system_name_strindex",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_INT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "systemNameStrindex",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "filename_strindex",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_INT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "filenameStrindex",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "start_line",
          extendee: nil,
          number: 4,
          label: :LABEL_OPTIONAL,
          type: :TYPE_INT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "startLine",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field :name_strindex, 1, type: :int32, json_name: "nameStrindex"
  field :system_name_strindex, 2, type: :int32, json_name: "systemNameStrindex"
  field :filename_strindex, 3, type: :int32, json_name: "filenameStrindex"
  field :start_line, 4, type: :int64, json_name: "startLine"
end