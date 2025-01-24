defmodule Opentelemetry.Proto.Metrics.V1.AggregationTemporality do
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

defmodule Opentelemetry.Proto.Metrics.V1.DataPointFlags do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.EnumDescriptorProto{
      name: "DataPointFlags",
      value: [
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "DATA_POINT_FLAGS_DO_NOT_USE",
          number: 0,
          options: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.EnumValueDescriptorProto{
          name: "DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK",
          number: 1,
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

  field :DATA_POINT_FLAGS_DO_NOT_USE, 0
  field :DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK, 1
end

defmodule Opentelemetry.Proto.Metrics.V1.MetricsData do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "MetricsData",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "resource_metrics",
          extendee: nil,
          number: 1,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.metrics.v1.ResourceMetrics",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "resourceMetrics",
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

  field :resource_metrics, 1,
    repeated: true,
    type: Opentelemetry.Proto.Metrics.V1.ResourceMetrics,
    json_name: "resourceMetrics"
end

defmodule Opentelemetry.Proto.Metrics.V1.ResourceMetrics do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "ResourceMetrics",
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
          name: "scope_metrics",
          extendee: nil,
          number: 2,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.metrics.v1.ScopeMetrics",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "scopeMetrics",
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

  field :scope_metrics, 2,
    repeated: true,
    type: Opentelemetry.Proto.Metrics.V1.ScopeMetrics,
    json_name: "scopeMetrics"

  field :schema_url, 3, type: :string, json_name: "schemaUrl"
end

defmodule Opentelemetry.Proto.Metrics.V1.ScopeMetrics do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "ScopeMetrics",
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
          name: "metrics",
          extendee: nil,
          number: 2,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.metrics.v1.Metric",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "metrics",
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
  field :metrics, 2, repeated: true, type: Opentelemetry.Proto.Metrics.V1.Metric
  field :schema_url, 3, type: :string, json_name: "schemaUrl"
end

defmodule Opentelemetry.Proto.Metrics.V1.Metric do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "Metric",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "name",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "name",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "description",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "description",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "unit",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "unit",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "gauge",
          extendee: nil,
          number: 5,
          label: :LABEL_OPTIONAL,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.metrics.v1.Gauge",
          default_value: nil,
          options: nil,
          oneof_index: 0,
          json_name: "gauge",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "sum",
          extendee: nil,
          number: 7,
          label: :LABEL_OPTIONAL,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.metrics.v1.Sum",
          default_value: nil,
          options: nil,
          oneof_index: 0,
          json_name: "sum",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "histogram",
          extendee: nil,
          number: 9,
          label: :LABEL_OPTIONAL,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.metrics.v1.Histogram",
          default_value: nil,
          options: nil,
          oneof_index: 0,
          json_name: "histogram",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "exponential_histogram",
          extendee: nil,
          number: 10,
          label: :LABEL_OPTIONAL,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.metrics.v1.ExponentialHistogram",
          default_value: nil,
          options: nil,
          oneof_index: 0,
          json_name: "exponentialHistogram",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "summary",
          extendee: nil,
          number: 11,
          label: :LABEL_OPTIONAL,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.metrics.v1.Summary",
          default_value: nil,
          options: nil,
          oneof_index: 0,
          json_name: "summary",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "metadata",
          extendee: nil,
          number: 12,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.common.v1.KeyValue",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "metadata",
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
        %Google.Protobuf.OneofDescriptorProto{name: "data", options: nil, __unknown_fields__: []}
      ],
      reserved_range: [
        %Google.Protobuf.DescriptorProto.ReservedRange{start: 4, end: 5, __unknown_fields__: []},
        %Google.Protobuf.DescriptorProto.ReservedRange{start: 6, end: 7, __unknown_fields__: []},
        %Google.Protobuf.DescriptorProto.ReservedRange{start: 8, end: 9, __unknown_fields__: []}
      ],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  oneof :data, 0

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

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "Gauge",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "data_points",
          extendee: nil,
          number: 1,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.metrics.v1.NumberDataPoint",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "dataPoints",
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

  field :data_points, 1,
    repeated: true,
    type: Opentelemetry.Proto.Metrics.V1.NumberDataPoint,
    json_name: "dataPoints"
end

defmodule Opentelemetry.Proto.Metrics.V1.Sum do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "Sum",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "data_points",
          extendee: nil,
          number: 1,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.metrics.v1.NumberDataPoint",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "dataPoints",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "aggregation_temporality",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_ENUM,
          type_name: ".opentelemetry.proto.metrics.v1.AggregationTemporality",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "aggregationTemporality",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "is_monotonic",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BOOL,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "isMonotonic",
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

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "Histogram",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "data_points",
          extendee: nil,
          number: 1,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.metrics.v1.HistogramDataPoint",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "dataPoints",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "aggregation_temporality",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_ENUM,
          type_name: ".opentelemetry.proto.metrics.v1.AggregationTemporality",
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

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "ExponentialHistogram",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "data_points",
          extendee: nil,
          number: 1,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "dataPoints",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "aggregation_temporality",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_ENUM,
          type_name: ".opentelemetry.proto.metrics.v1.AggregationTemporality",
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

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "Summary",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "data_points",
          extendee: nil,
          number: 1,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.metrics.v1.SummaryDataPoint",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "dataPoints",
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

  field :data_points, 1,
    repeated: true,
    type: Opentelemetry.Proto.Metrics.V1.SummaryDataPoint,
    json_name: "dataPoints"
end

defmodule Opentelemetry.Proto.Metrics.V1.NumberDataPoint do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "NumberDataPoint",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "attributes",
          extendee: nil,
          number: 7,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.common.v1.KeyValue",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "attributes",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "start_time_unix_nano",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_FIXED64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "startTimeUnixNano",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "time_unix_nano",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_FIXED64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "timeUnixNano",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "as_double",
          extendee: nil,
          number: 4,
          label: :LABEL_OPTIONAL,
          type: :TYPE_DOUBLE,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: 0,
          json_name: "asDouble",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "as_int",
          extendee: nil,
          number: 6,
          label: :LABEL_OPTIONAL,
          type: :TYPE_SFIXED64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: 0,
          json_name: "asInt",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "exemplars",
          extendee: nil,
          number: 5,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.metrics.v1.Exemplar",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "exemplars",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "flags",
          extendee: nil,
          number: 8,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "flags",
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
        %Google.Protobuf.OneofDescriptorProto{name: "value", options: nil, __unknown_fields__: []}
      ],
      reserved_range: [
        %Google.Protobuf.DescriptorProto.ReservedRange{start: 1, end: 2, __unknown_fields__: []}
      ],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  oneof :value, 0

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

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "HistogramDataPoint",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "attributes",
          extendee: nil,
          number: 9,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.common.v1.KeyValue",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "attributes",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "start_time_unix_nano",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_FIXED64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "startTimeUnixNano",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "time_unix_nano",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_FIXED64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "timeUnixNano",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "count",
          extendee: nil,
          number: 4,
          label: :LABEL_OPTIONAL,
          type: :TYPE_FIXED64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "count",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "sum",
          extendee: nil,
          number: 5,
          label: :LABEL_OPTIONAL,
          type: :TYPE_DOUBLE,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: 0,
          json_name: "sum",
          proto3_optional: true,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "bucket_counts",
          extendee: nil,
          number: 6,
          label: :LABEL_REPEATED,
          type: :TYPE_FIXED64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "bucketCounts",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "explicit_bounds",
          extendee: nil,
          number: 7,
          label: :LABEL_REPEATED,
          type: :TYPE_DOUBLE,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "explicitBounds",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "exemplars",
          extendee: nil,
          number: 8,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.metrics.v1.Exemplar",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "exemplars",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "flags",
          extendee: nil,
          number: 10,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "flags",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "min",
          extendee: nil,
          number: 11,
          label: :LABEL_OPTIONAL,
          type: :TYPE_DOUBLE,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: 1,
          json_name: "min",
          proto3_optional: true,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "max",
          extendee: nil,
          number: 12,
          label: :LABEL_OPTIONAL,
          type: :TYPE_DOUBLE,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: 2,
          json_name: "max",
          proto3_optional: true,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [
        %Google.Protobuf.OneofDescriptorProto{name: "_sum", options: nil, __unknown_fields__: []},
        %Google.Protobuf.OneofDescriptorProto{name: "_min", options: nil, __unknown_fields__: []},
        %Google.Protobuf.OneofDescriptorProto{name: "_max", options: nil, __unknown_fields__: []}
      ],
      reserved_range: [
        %Google.Protobuf.DescriptorProto.ReservedRange{start: 1, end: 2, __unknown_fields__: []}
      ],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

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

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "Buckets",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "offset",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_SINT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "offset",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "bucket_counts",
          extendee: nil,
          number: 2,
          label: :LABEL_REPEATED,
          type: :TYPE_UINT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "bucketCounts",
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

  field :offset, 1, type: :sint32
  field :bucket_counts, 2, repeated: true, type: :uint64, json_name: "bucketCounts"
end

defmodule Opentelemetry.Proto.Metrics.V1.ExponentialHistogramDataPoint do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "ExponentialHistogramDataPoint",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "attributes",
          extendee: nil,
          number: 1,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.common.v1.KeyValue",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "attributes",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "start_time_unix_nano",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_FIXED64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "startTimeUnixNano",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "time_unix_nano",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_FIXED64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "timeUnixNano",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "count",
          extendee: nil,
          number: 4,
          label: :LABEL_OPTIONAL,
          type: :TYPE_FIXED64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "count",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "sum",
          extendee: nil,
          number: 5,
          label: :LABEL_OPTIONAL,
          type: :TYPE_DOUBLE,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: 0,
          json_name: "sum",
          proto3_optional: true,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "scale",
          extendee: nil,
          number: 6,
          label: :LABEL_OPTIONAL,
          type: :TYPE_SINT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "scale",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "zero_count",
          extendee: nil,
          number: 7,
          label: :LABEL_OPTIONAL,
          type: :TYPE_FIXED64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "zeroCount",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "positive",
          extendee: nil,
          number: 8,
          label: :LABEL_OPTIONAL,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint.Buckets",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "positive",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "negative",
          extendee: nil,
          number: 9,
          label: :LABEL_OPTIONAL,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint.Buckets",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "negative",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "flags",
          extendee: nil,
          number: 10,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "flags",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "exemplars",
          extendee: nil,
          number: 11,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.metrics.v1.Exemplar",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "exemplars",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "min",
          extendee: nil,
          number: 12,
          label: :LABEL_OPTIONAL,
          type: :TYPE_DOUBLE,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: 1,
          json_name: "min",
          proto3_optional: true,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "max",
          extendee: nil,
          number: 13,
          label: :LABEL_OPTIONAL,
          type: :TYPE_DOUBLE,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: 2,
          json_name: "max",
          proto3_optional: true,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "zero_threshold",
          extendee: nil,
          number: 14,
          label: :LABEL_OPTIONAL,
          type: :TYPE_DOUBLE,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "zeroThreshold",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [
        %Google.Protobuf.DescriptorProto{
          name: "Buckets",
          field: [
            %Google.Protobuf.FieldDescriptorProto{
              name: "offset",
              extendee: nil,
              number: 1,
              label: :LABEL_OPTIONAL,
              type: :TYPE_SINT32,
              type_name: nil,
              default_value: nil,
              options: nil,
              oneof_index: nil,
              json_name: "offset",
              proto3_optional: nil,
              __unknown_fields__: []
            },
            %Google.Protobuf.FieldDescriptorProto{
              name: "bucket_counts",
              extendee: nil,
              number: 2,
              label: :LABEL_REPEATED,
              type: :TYPE_UINT64,
              type_name: nil,
              default_value: nil,
              options: nil,
              oneof_index: nil,
              json_name: "bucketCounts",
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
      ],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [
        %Google.Protobuf.OneofDescriptorProto{name: "_sum", options: nil, __unknown_fields__: []},
        %Google.Protobuf.OneofDescriptorProto{name: "_min", options: nil, __unknown_fields__: []},
        %Google.Protobuf.OneofDescriptorProto{name: "_max", options: nil, __unknown_fields__: []}
      ],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

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

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "ValueAtQuantile",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "quantile",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_DOUBLE,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "quantile",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "value",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_DOUBLE,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "value",
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

  field :quantile, 1, type: :double
  field :value, 2, type: :double
end

defmodule Opentelemetry.Proto.Metrics.V1.SummaryDataPoint do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "SummaryDataPoint",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "attributes",
          extendee: nil,
          number: 7,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.common.v1.KeyValue",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "attributes",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "start_time_unix_nano",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_FIXED64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "startTimeUnixNano",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "time_unix_nano",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_FIXED64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "timeUnixNano",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "count",
          extendee: nil,
          number: 4,
          label: :LABEL_OPTIONAL,
          type: :TYPE_FIXED64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "count",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "sum",
          extendee: nil,
          number: 5,
          label: :LABEL_OPTIONAL,
          type: :TYPE_DOUBLE,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "sum",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "quantile_values",
          extendee: nil,
          number: 6,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.metrics.v1.SummaryDataPoint.ValueAtQuantile",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "quantileValues",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "flags",
          extendee: nil,
          number: 8,
          label: :LABEL_OPTIONAL,
          type: :TYPE_UINT32,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "flags",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [
        %Google.Protobuf.DescriptorProto{
          name: "ValueAtQuantile",
          field: [
            %Google.Protobuf.FieldDescriptorProto{
              name: "quantile",
              extendee: nil,
              number: 1,
              label: :LABEL_OPTIONAL,
              type: :TYPE_DOUBLE,
              type_name: nil,
              default_value: nil,
              options: nil,
              oneof_index: nil,
              json_name: "quantile",
              proto3_optional: nil,
              __unknown_fields__: []
            },
            %Google.Protobuf.FieldDescriptorProto{
              name: "value",
              extendee: nil,
              number: 2,
              label: :LABEL_OPTIONAL,
              type: :TYPE_DOUBLE,
              type_name: nil,
              default_value: nil,
              options: nil,
              oneof_index: nil,
              json_name: "value",
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
      ],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [],
      reserved_range: [
        %Google.Protobuf.DescriptorProto.ReservedRange{start: 1, end: 2, __unknown_fields__: []}
      ],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

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

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "Exemplar",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "filtered_attributes",
          extendee: nil,
          number: 7,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.common.v1.KeyValue",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "filteredAttributes",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "time_unix_nano",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_FIXED64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "timeUnixNano",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "as_double",
          extendee: nil,
          number: 3,
          label: :LABEL_OPTIONAL,
          type: :TYPE_DOUBLE,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: 0,
          json_name: "asDouble",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "as_int",
          extendee: nil,
          number: 6,
          label: :LABEL_OPTIONAL,
          type: :TYPE_SFIXED64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: 0,
          json_name: "asInt",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "span_id",
          extendee: nil,
          number: 4,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BYTES,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "spanId",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "trace_id",
          extendee: nil,
          number: 5,
          label: :LABEL_OPTIONAL,
          type: :TYPE_BYTES,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "traceId",
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
        %Google.Protobuf.OneofDescriptorProto{name: "value", options: nil, __unknown_fields__: []}
      ],
      reserved_range: [
        %Google.Protobuf.DescriptorProto.ReservedRange{start: 1, end: 2, __unknown_fields__: []}
      ],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  oneof :value, 0

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