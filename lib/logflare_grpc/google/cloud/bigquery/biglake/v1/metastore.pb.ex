defmodule Google.Cloud.Bigquery.Biglake.V1.TableView do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :TABLE_VIEW_UNSPECIFIED, 0
  field :BASIC, 1
  field :FULL, 2
end

defmodule Google.Cloud.Bigquery.Biglake.V1.Database.Type do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :TYPE_UNSPECIFIED, 0
  field :HIVE, 1
end

defmodule Google.Cloud.Bigquery.Biglake.V1.Table.Type do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :TYPE_UNSPECIFIED, 0
  field :HIVE, 1
end

defmodule Google.Cloud.Bigquery.Biglake.V1.Catalog do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false

  field :create_time, 2,
    type: Google.Protobuf.Timestamp,
    json_name: "createTime",
    deprecated: false

  field :update_time, 3,
    type: Google.Protobuf.Timestamp,
    json_name: "updateTime",
    deprecated: false

  field :delete_time, 4,
    type: Google.Protobuf.Timestamp,
    json_name: "deleteTime",
    deprecated: false

  field :expire_time, 5,
    type: Google.Protobuf.Timestamp,
    json_name: "expireTime",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Biglake.V1.Database do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:options, 0)

  field :hive_options, 7,
    type: Google.Cloud.Bigquery.Biglake.V1.HiveDatabaseOptions,
    json_name: "hiveOptions",
    oneof: 0

  field :name, 1, type: :string, deprecated: false

  field :create_time, 2,
    type: Google.Protobuf.Timestamp,
    json_name: "createTime",
    deprecated: false

  field :update_time, 3,
    type: Google.Protobuf.Timestamp,
    json_name: "updateTime",
    deprecated: false

  field :delete_time, 4,
    type: Google.Protobuf.Timestamp,
    json_name: "deleteTime",
    deprecated: false

  field :expire_time, 5,
    type: Google.Protobuf.Timestamp,
    json_name: "expireTime",
    deprecated: false

  field :type, 6, type: Google.Cloud.Bigquery.Biglake.V1.Database.Type, enum: true
end

defmodule Google.Cloud.Bigquery.Biglake.V1.Table do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:options, 0)

  field :hive_options, 7,
    type: Google.Cloud.Bigquery.Biglake.V1.HiveTableOptions,
    json_name: "hiveOptions",
    oneof: 0

  field :name, 1, type: :string, deprecated: false

  field :create_time, 2,
    type: Google.Protobuf.Timestamp,
    json_name: "createTime",
    deprecated: false

  field :update_time, 3,
    type: Google.Protobuf.Timestamp,
    json_name: "updateTime",
    deprecated: false

  field :delete_time, 4,
    type: Google.Protobuf.Timestamp,
    json_name: "deleteTime",
    deprecated: false

  field :expire_time, 5,
    type: Google.Protobuf.Timestamp,
    json_name: "expireTime",
    deprecated: false

  field :type, 6, type: Google.Cloud.Bigquery.Biglake.V1.Table.Type, enum: true
  field :etag, 8, type: :string
end

defmodule Google.Cloud.Bigquery.Biglake.V1.CreateCatalogRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false
  field :catalog, 2, type: Google.Cloud.Bigquery.Biglake.V1.Catalog, deprecated: false
  field :catalog_id, 3, type: :string, json_name: "catalogId", deprecated: false
end

defmodule Google.Cloud.Bigquery.Biglake.V1.DeleteCatalogRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Biglake.V1.GetCatalogRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Biglake.V1.ListCatalogsRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false
  field :page_size, 2, type: :int32, json_name: "pageSize"
  field :page_token, 3, type: :string, json_name: "pageToken"
end

defmodule Google.Cloud.Bigquery.Biglake.V1.ListCatalogsResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :catalogs, 1, repeated: true, type: Google.Cloud.Bigquery.Biglake.V1.Catalog
  field :next_page_token, 2, type: :string, json_name: "nextPageToken"
end

defmodule Google.Cloud.Bigquery.Biglake.V1.CreateDatabaseRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false
  field :database, 2, type: Google.Cloud.Bigquery.Biglake.V1.Database, deprecated: false
  field :database_id, 3, type: :string, json_name: "databaseId", deprecated: false
end

defmodule Google.Cloud.Bigquery.Biglake.V1.DeleteDatabaseRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Biglake.V1.UpdateDatabaseRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :database, 1, type: Google.Cloud.Bigquery.Biglake.V1.Database, deprecated: false
  field :update_mask, 2, type: Google.Protobuf.FieldMask, json_name: "updateMask"
end

defmodule Google.Cloud.Bigquery.Biglake.V1.GetDatabaseRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Biglake.V1.ListDatabasesRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false
  field :page_size, 2, type: :int32, json_name: "pageSize"
  field :page_token, 3, type: :string, json_name: "pageToken"
end

defmodule Google.Cloud.Bigquery.Biglake.V1.ListDatabasesResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :databases, 1, repeated: true, type: Google.Cloud.Bigquery.Biglake.V1.Database
  field :next_page_token, 2, type: :string, json_name: "nextPageToken"
end

defmodule Google.Cloud.Bigquery.Biglake.V1.CreateTableRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false
  field :table, 2, type: Google.Cloud.Bigquery.Biglake.V1.Table, deprecated: false
  field :table_id, 3, type: :string, json_name: "tableId", deprecated: false
end

defmodule Google.Cloud.Bigquery.Biglake.V1.DeleteTableRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Biglake.V1.UpdateTableRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :table, 1, type: Google.Cloud.Bigquery.Biglake.V1.Table, deprecated: false
  field :update_mask, 2, type: Google.Protobuf.FieldMask, json_name: "updateMask"
end

defmodule Google.Cloud.Bigquery.Biglake.V1.RenameTableRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
  field :new_name, 2, type: :string, json_name: "newName", deprecated: false
end

defmodule Google.Cloud.Bigquery.Biglake.V1.GetTableRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Biglake.V1.ListTablesRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false
  field :page_size, 2, type: :int32, json_name: "pageSize"
  field :page_token, 3, type: :string, json_name: "pageToken"
  field :view, 4, type: Google.Cloud.Bigquery.Biglake.V1.TableView, enum: true
end

defmodule Google.Cloud.Bigquery.Biglake.V1.ListTablesResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :tables, 1, repeated: true, type: Google.Cloud.Bigquery.Biglake.V1.Table
  field :next_page_token, 2, type: :string, json_name: "nextPageToken"
end

defmodule Google.Cloud.Bigquery.Biglake.V1.HiveDatabaseOptions.ParametersEntry do
  @moduledoc false

  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Google.Cloud.Bigquery.Biglake.V1.HiveDatabaseOptions do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :location_uri, 1, type: :string, json_name: "locationUri"

  field :parameters, 2,
    repeated: true,
    type: Google.Cloud.Bigquery.Biglake.V1.HiveDatabaseOptions.ParametersEntry,
    map: true
end

defmodule Google.Cloud.Bigquery.Biglake.V1.HiveTableOptions.SerDeInfo do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :serialization_lib, 1, type: :string, json_name: "serializationLib"
end

defmodule Google.Cloud.Bigquery.Biglake.V1.HiveTableOptions.StorageDescriptor do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :location_uri, 1, type: :string, json_name: "locationUri"
  field :input_format, 2, type: :string, json_name: "inputFormat"
  field :output_format, 3, type: :string, json_name: "outputFormat"

  field :serde_info, 4,
    type: Google.Cloud.Bigquery.Biglake.V1.HiveTableOptions.SerDeInfo,
    json_name: "serdeInfo"
end

defmodule Google.Cloud.Bigquery.Biglake.V1.HiveTableOptions.ParametersEntry do
  @moduledoc false

  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Google.Cloud.Bigquery.Biglake.V1.HiveTableOptions do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parameters, 1,
    repeated: true,
    type: Google.Cloud.Bigquery.Biglake.V1.HiveTableOptions.ParametersEntry,
    map: true

  field :table_type, 2, type: :string, json_name: "tableType"

  field :storage_descriptor, 3,
    type: Google.Cloud.Bigquery.Biglake.V1.HiveTableOptions.StorageDescriptor,
    json_name: "storageDescriptor"
end

defmodule Google.Cloud.Bigquery.Biglake.V1.MetastoreService.Service do
  @moduledoc false

  use GRPC.Service,
    name: "google.cloud.bigquery.biglake.v1.MetastoreService",
    protoc_gen_elixir_version: "0.13.0"

  rpc(
    :CreateCatalog,
    Google.Cloud.Bigquery.Biglake.V1.CreateCatalogRequest,
    Google.Cloud.Bigquery.Biglake.V1.Catalog
  )

  rpc(
    :DeleteCatalog,
    Google.Cloud.Bigquery.Biglake.V1.DeleteCatalogRequest,
    Google.Cloud.Bigquery.Biglake.V1.Catalog
  )

  rpc(
    :GetCatalog,
    Google.Cloud.Bigquery.Biglake.V1.GetCatalogRequest,
    Google.Cloud.Bigquery.Biglake.V1.Catalog
  )

  rpc(
    :ListCatalogs,
    Google.Cloud.Bigquery.Biglake.V1.ListCatalogsRequest,
    Google.Cloud.Bigquery.Biglake.V1.ListCatalogsResponse
  )

  rpc(
    :CreateDatabase,
    Google.Cloud.Bigquery.Biglake.V1.CreateDatabaseRequest,
    Google.Cloud.Bigquery.Biglake.V1.Database
  )

  rpc(
    :DeleteDatabase,
    Google.Cloud.Bigquery.Biglake.V1.DeleteDatabaseRequest,
    Google.Cloud.Bigquery.Biglake.V1.Database
  )

  rpc(
    :UpdateDatabase,
    Google.Cloud.Bigquery.Biglake.V1.UpdateDatabaseRequest,
    Google.Cloud.Bigquery.Biglake.V1.Database
  )

  rpc(
    :GetDatabase,
    Google.Cloud.Bigquery.Biglake.V1.GetDatabaseRequest,
    Google.Cloud.Bigquery.Biglake.V1.Database
  )

  rpc(
    :ListDatabases,
    Google.Cloud.Bigquery.Biglake.V1.ListDatabasesRequest,
    Google.Cloud.Bigquery.Biglake.V1.ListDatabasesResponse
  )

  rpc(
    :CreateTable,
    Google.Cloud.Bigquery.Biglake.V1.CreateTableRequest,
    Google.Cloud.Bigquery.Biglake.V1.Table
  )

  rpc(
    :DeleteTable,
    Google.Cloud.Bigquery.Biglake.V1.DeleteTableRequest,
    Google.Cloud.Bigquery.Biglake.V1.Table
  )

  rpc(
    :UpdateTable,
    Google.Cloud.Bigquery.Biglake.V1.UpdateTableRequest,
    Google.Cloud.Bigquery.Biglake.V1.Table
  )

  rpc(
    :RenameTable,
    Google.Cloud.Bigquery.Biglake.V1.RenameTableRequest,
    Google.Cloud.Bigquery.Biglake.V1.Table
  )

  rpc(
    :GetTable,
    Google.Cloud.Bigquery.Biglake.V1.GetTableRequest,
    Google.Cloud.Bigquery.Biglake.V1.Table
  )

  rpc(
    :ListTables,
    Google.Cloud.Bigquery.Biglake.V1.ListTablesRequest,
    Google.Cloud.Bigquery.Biglake.V1.ListTablesResponse
  )
end

defmodule Google.Cloud.Bigquery.Biglake.V1.MetastoreService.Stub do
  @moduledoc false

  use GRPC.Stub, service: Google.Cloud.Bigquery.Biglake.V1.MetastoreService.Service
end
