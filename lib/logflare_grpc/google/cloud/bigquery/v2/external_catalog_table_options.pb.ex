defmodule Google.Cloud.Bigquery.V2.ExternalCatalogTableOptions.ParametersEntry do
  @moduledoc false

  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Google.Cloud.Bigquery.V2.ExternalCatalogTableOptions do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parameters, 1,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.ExternalCatalogTableOptions.ParametersEntry,
    map: true,
    deprecated: false

  field :storage_descriptor, 2,
    type: Google.Cloud.Bigquery.V2.StorageDescriptor,
    json_name: "storageDescriptor",
    deprecated: false

  field :connection_id, 3, type: :string, json_name: "connectionId", deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.StorageDescriptor do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :location_uri, 1, type: :string, json_name: "locationUri", deprecated: false
  field :input_format, 2, type: :string, json_name: "inputFormat", deprecated: false
  field :output_format, 3, type: :string, json_name: "outputFormat", deprecated: false

  field :serde_info, 4,
    type: Google.Cloud.Bigquery.V2.SerDeInfo,
    json_name: "serdeInfo",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.SerDeInfo.ParametersEntry do
  @moduledoc false

  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Google.Cloud.Bigquery.V2.SerDeInfo do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false

  field :serialization_library, 2,
    type: :string,
    json_name: "serializationLibrary",
    deprecated: false

  field :parameters, 3,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.SerDeInfo.ParametersEntry,
    map: true,
    deprecated: false
end
