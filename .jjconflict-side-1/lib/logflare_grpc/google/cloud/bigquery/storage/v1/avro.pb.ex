defmodule Google.Cloud.Bigquery.Storage.V1.AvroSchema do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :schema, 1, type: :string
end

defmodule Google.Cloud.Bigquery.Storage.V1.AvroRows do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :serialized_binary_rows, 1, type: :bytes, json_name: "serializedBinaryRows"
  field :row_count, 2, type: :int64, json_name: "rowCount", deprecated: true
end

defmodule Google.Cloud.Bigquery.Storage.V1.AvroSerializationOptions do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :enable_display_name_attribute, 1, type: :bool, json_name: "enableDisplayNameAttribute"
end
