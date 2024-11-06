defmodule Opentelemetry.Proto.Resource.V1.Resource do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :attributes, 1, repeated: true, type: Opentelemetry.Proto.Common.V1.KeyValue
  field :dropped_attributes_count, 2, type: :uint32, json_name: "droppedAttributesCount"
end
