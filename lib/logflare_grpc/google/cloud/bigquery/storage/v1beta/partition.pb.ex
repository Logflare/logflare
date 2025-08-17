defmodule Google.Cloud.Bigquery.Storage.V1beta.FieldSchema do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
  field :type, 2, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1beta.StorageDescriptor do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :location_uri, 1, type: :string, json_name: "locationUri", deprecated: false
  field :input_format, 2, type: :string, json_name: "inputFormat", deprecated: false
  field :output_format, 3, type: :string, json_name: "outputFormat", deprecated: false

  field :serde_info, 4,
    type: Google.Cloud.Bigquery.Storage.V1beta.SerDeInfo,
    json_name: "serdeInfo",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1beta.SerDeInfo.ParametersEntry do
  @moduledoc false

  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Google.Cloud.Bigquery.Storage.V1beta.SerDeInfo do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false

  field :serialization_library, 2,
    type: :string,
    json_name: "serializationLibrary",
    deprecated: false

  field :parameters, 3,
    repeated: true,
    type: Google.Cloud.Bigquery.Storage.V1beta.SerDeInfo.ParametersEntry,
    map: true,
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1beta.MetastorePartition.ParametersEntry do
  @moduledoc false

  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Google.Cloud.Bigquery.Storage.V1beta.MetastorePartition do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :values, 1, repeated: true, type: :string, deprecated: false

  field :create_time, 2,
    type: Google.Protobuf.Timestamp,
    json_name: "createTime",
    deprecated: false

  field :storage_descriptor, 3,
    type: Google.Cloud.Bigquery.Storage.V1beta.StorageDescriptor,
    json_name: "storageDescriptor",
    deprecated: false

  field :parameters, 4,
    repeated: true,
    type: Google.Cloud.Bigquery.Storage.V1beta.MetastorePartition.ParametersEntry,
    map: true,
    deprecated: false

  field :fields, 5,
    repeated: true,
    type: Google.Cloud.Bigquery.Storage.V1beta.FieldSchema,
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1beta.MetastorePartitionList do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :partitions, 1,
    repeated: true,
    type: Google.Cloud.Bigquery.Storage.V1beta.MetastorePartition,
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1beta.ReadStream do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1beta.StreamList do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :streams, 1,
    repeated: true,
    type: Google.Cloud.Bigquery.Storage.V1beta.ReadStream,
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1beta.MetastorePartitionValues do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :values, 1, repeated: true, type: :string, deprecated: false
end
