defmodule Google.Cloud.Bigquery.V2.BigLakeConfiguration.FileFormat do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :FILE_FORMAT_UNSPECIFIED, 0
  field :PARQUET, 1
end

defmodule Google.Cloud.Bigquery.V2.BigLakeConfiguration.TableFormat do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :TABLE_FORMAT_UNSPECIFIED, 0
  field :ICEBERG, 1
end

defmodule Google.Cloud.Bigquery.V2.BigLakeConfiguration do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :connection_id, 1, type: :string, json_name: "connectionId", deprecated: false
  field :storage_uri, 2, type: :string, json_name: "storageUri", deprecated: false

  field :file_format, 3,
    type: Google.Cloud.Bigquery.V2.BigLakeConfiguration.FileFormat,
    json_name: "fileFormat",
    enum: true,
    deprecated: false

  field :table_format, 4,
    type: Google.Cloud.Bigquery.V2.BigLakeConfiguration.TableFormat,
    json_name: "tableFormat",
    enum: true,
    deprecated: false
end
