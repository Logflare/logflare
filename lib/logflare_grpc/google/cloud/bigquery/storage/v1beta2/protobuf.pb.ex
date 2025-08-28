defmodule Google.Cloud.Bigquery.Storage.V1beta2.ProtoSchema do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :proto_descriptor, 1, type: Google.Protobuf.DescriptorProto, json_name: "protoDescriptor"
end

defmodule Google.Cloud.Bigquery.Storage.V1beta2.ProtoRows do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :serialized_rows, 1, repeated: true, type: :bytes, json_name: "serializedRows"
end
