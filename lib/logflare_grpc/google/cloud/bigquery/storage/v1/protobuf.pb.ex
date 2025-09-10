defmodule Google.Cloud.Bigquery.Storage.V1.ProtoSchema do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :proto_descriptor, 1, type: Google.Protobuf.DescriptorProto, json_name: "protoDescriptor"
end

defmodule Google.Cloud.Bigquery.Storage.V1.ProtoRows do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :serialized_rows, 1, repeated: true, type: :bytes, json_name: "serializedRows"
end
