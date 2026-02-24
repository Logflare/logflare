defmodule Opentelemetry.Proto.Resource.V1.Resource do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :attributes, 1, repeated: true, type: Opentelemetry.Proto.Common.V1.KeyValue
  field :dropped_attributes_count, 2, type: :uint32, json_name: "droppedAttributesCount"

  field :entity_refs, 3,
    repeated: true,
    type: Opentelemetry.Proto.Common.V1.EntityRef,
    json_name: "entityRefs"
end
