defmodule Google.Cloud.Bigquery.Storage.V1alpha.FieldSchema do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
  field :type, 2, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1alpha.StorageDescriptor do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :location_uri, 1, type: :string, json_name: "locationUri", deprecated: false
  field :input_format, 2, type: :string, json_name: "inputFormat", deprecated: false
  field :output_format, 3, type: :string, json_name: "outputFormat", deprecated: false

  field :serde_info, 4,
    type: Google.Cloud.Bigquery.Storage.V1alpha.SerDeInfo,
    json_name: "serdeInfo",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1alpha.SerDeInfo.ParametersEntry do
  @moduledoc false

  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Google.Cloud.Bigquery.Storage.V1alpha.SerDeInfo do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false

  field :serialization_library, 2,
    type: :string,
    json_name: "serializationLibrary",
    deprecated: false

  field :parameters, 3,
    repeated: true,
    type: Google.Cloud.Bigquery.Storage.V1alpha.SerDeInfo.ParametersEntry,
    map: true,
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1alpha.MetastorePartition.ParametersEntry do
  @moduledoc false

  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Google.Cloud.Bigquery.Storage.V1alpha.MetastorePartition do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :values, 1, repeated: true, type: :string, deprecated: false

  field :create_time, 2,
    type: Google.Protobuf.Timestamp,
    json_name: "createTime",
    deprecated: false

  field :storage_descriptor, 3,
    type: Google.Cloud.Bigquery.Storage.V1alpha.StorageDescriptor,
    json_name: "storageDescriptor",
    deprecated: false

  field :parameters, 4,
    repeated: true,
    type: Google.Cloud.Bigquery.Storage.V1alpha.MetastorePartition.ParametersEntry,
    map: true,
    deprecated: false

  field :fields, 5,
    repeated: true,
    type: Google.Cloud.Bigquery.Storage.V1alpha.FieldSchema,
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1alpha.MetastorePartitionList do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :partitions, 1,
    repeated: true,
    type: Google.Cloud.Bigquery.Storage.V1alpha.MetastorePartition,
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1alpha.ReadStream do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1alpha.StreamList do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :streams, 1,
    repeated: true,
    type: Google.Cloud.Bigquery.Storage.V1alpha.ReadStream,
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1alpha.MetastorePartitionValues do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :values, 1, repeated: true, type: :string, deprecated: false
end
