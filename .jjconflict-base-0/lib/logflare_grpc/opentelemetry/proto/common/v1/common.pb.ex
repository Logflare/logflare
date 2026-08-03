defmodule Opentelemetry.Proto.Common.V1.AnyValue do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  oneof(:value, 0)

  field :string_value, 1, type: :string, json_name: "stringValue", oneof: 0
  field :bool_value, 2, type: :bool, json_name: "boolValue", oneof: 0
  field :int_value, 3, type: :int64, json_name: "intValue", oneof: 0
  field :double_value, 4, type: :double, json_name: "doubleValue", oneof: 0

  field :array_value, 5,
    type: Opentelemetry.Proto.Common.V1.ArrayValue,
    json_name: "arrayValue",
    oneof: 0

  field :kvlist_value, 6,
    type: Opentelemetry.Proto.Common.V1.KeyValueList,
    json_name: "kvlistValue",
    oneof: 0

  field :bytes_value, 7, type: :bytes, json_name: "bytesValue", oneof: 0
end

defmodule Opentelemetry.Proto.Common.V1.ArrayValue do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :values, 1, repeated: true, type: Opentelemetry.Proto.Common.V1.AnyValue
end

defmodule Opentelemetry.Proto.Common.V1.KeyValueList do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :values, 1, repeated: true, type: Opentelemetry.Proto.Common.V1.KeyValue
end

defmodule Opentelemetry.Proto.Common.V1.KeyValue do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :key, 1, type: :string
  field :value, 2, type: Opentelemetry.Proto.Common.V1.AnyValue
end

defmodule Opentelemetry.Proto.Common.V1.InstrumentationScope do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :name, 1, type: :string
  field :version, 2, type: :string
  field :attributes, 3, repeated: true, type: Opentelemetry.Proto.Common.V1.KeyValue
  field :dropped_attributes_count, 4, type: :uint32, json_name: "droppedAttributesCount"
end

defmodule Opentelemetry.Proto.Common.V1.EntityRef do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :schema_url, 1, type: :string, json_name: "schemaUrl"
  field :type, 2, type: :string
  field :id_keys, 3, repeated: true, type: :string, json_name: "idKeys"
  field :description_keys, 4, repeated: true, type: :string, json_name: "descriptionKeys"
end
