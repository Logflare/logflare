defmodule Google.Cloud.Bigquery.V2.TableReplicationInfo.ReplicationStatus do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :REPLICATION_STATUS_UNSPECIFIED, 0
  field :ACTIVE, 1
  field :SOURCE_DELETED, 2
  field :PERMISSION_DENIED, 3
  field :UNSUPPORTED_CONFIGURATION, 4
end

defmodule Google.Cloud.Bigquery.V2.GetTableRequest.TableMetadataView do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :TABLE_METADATA_VIEW_UNSPECIFIED, 0
  field :BASIC, 1
  field :STORAGE_STATS, 2
  field :FULL, 3
end

defmodule Google.Cloud.Bigquery.V2.TableReplicationInfo do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :source_table, 1,
    type: Google.Cloud.Bigquery.V2.TableReference,
    json_name: "sourceTable",
    deprecated: false

  field :replication_interval_ms, 2,
    type: :int64,
    json_name: "replicationIntervalMs",
    deprecated: false

  field :replicated_source_last_refresh_time, 3,
    type: :int64,
    json_name: "replicatedSourceLastRefreshTime",
    deprecated: false

  field :replication_status, 4,
    type: Google.Cloud.Bigquery.V2.TableReplicationInfo.ReplicationStatus,
    json_name: "replicationStatus",
    enum: true,
    deprecated: false

  field :replication_error, 5,
    type: Google.Cloud.Bigquery.V2.ErrorProto,
    json_name: "replicationError",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.ViewDefinition do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :query, 1, type: :string, deprecated: false

  field :user_defined_function_resources, 2,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.UserDefinedFunctionResource,
    json_name: "userDefinedFunctionResources"

  field :use_legacy_sql, 3, type: Google.Protobuf.BoolValue, json_name: "useLegacySql"
  field :use_explicit_column_names, 4, type: :bool, json_name: "useExplicitColumnNames"

  field :privacy_policy, 5,
    type: Google.Cloud.Bigquery.V2.PrivacyPolicy,
    json_name: "privacyPolicy",
    deprecated: false

  field :foreign_definitions, 6,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.ForeignViewDefinition,
    json_name: "foreignDefinitions",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.ForeignViewDefinition do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :query, 1, type: :string, deprecated: false
  field :dialect, 7, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.MaterializedViewDefinition do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :query, 1, type: :string, deprecated: false
  field :last_refresh_time, 2, type: :int64, json_name: "lastRefreshTime", deprecated: false

  field :enable_refresh, 3,
    type: Google.Protobuf.BoolValue,
    json_name: "enableRefresh",
    deprecated: false

  field :refresh_interval_ms, 4,
    type: Google.Protobuf.UInt64Value,
    json_name: "refreshIntervalMs",
    deprecated: false

  field :allow_non_incremental_definition, 6,
    type: Google.Protobuf.BoolValue,
    json_name: "allowNonIncrementalDefinition",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.MaterializedViewStatus do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :refresh_watermark, 1,
    type: Google.Protobuf.Timestamp,
    json_name: "refreshWatermark",
    deprecated: false

  field :last_refresh_status, 2,
    type: Google.Cloud.Bigquery.V2.ErrorProto,
    json_name: "lastRefreshStatus",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.SnapshotDefinition do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :base_table_reference, 1,
    type: Google.Cloud.Bigquery.V2.TableReference,
    json_name: "baseTableReference",
    deprecated: false

  field :snapshot_time, 2,
    type: Google.Protobuf.Timestamp,
    json_name: "snapshotTime",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.CloneDefinition do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :base_table_reference, 1,
    type: Google.Cloud.Bigquery.V2.TableReference,
    json_name: "baseTableReference",
    deprecated: false

  field :clone_time, 2, type: Google.Protobuf.Timestamp, json_name: "cloneTime", deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.Streamingbuffer do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :estimated_bytes, 1, type: :uint64, json_name: "estimatedBytes", deprecated: false
  field :estimated_rows, 2, type: :uint64, json_name: "estimatedRows", deprecated: false
  field :oldest_entry_time, 3, type: :fixed64, json_name: "oldestEntryTime", deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.Table.LabelsEntry do
  @moduledoc false

  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Google.Cloud.Bigquery.V2.Table.ResourceTagsEntry do
  @moduledoc false

  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Google.Cloud.Bigquery.V2.Table do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :kind, 1, type: :string
  field :etag, 2, type: :string, deprecated: false
  field :id, 3, type: :string, deprecated: false
  field :self_link, 4, type: :string, json_name: "selfLink", deprecated: false

  field :table_reference, 5,
    type: Google.Cloud.Bigquery.V2.TableReference,
    json_name: "tableReference",
    deprecated: false

  field :friendly_name, 6,
    type: Google.Protobuf.StringValue,
    json_name: "friendlyName",
    deprecated: false

  field :description, 7, type: Google.Protobuf.StringValue, deprecated: false
  field :labels, 8, repeated: true, type: Google.Cloud.Bigquery.V2.Table.LabelsEntry, map: true
  field :schema, 9, type: Google.Cloud.Bigquery.V2.TableSchema, deprecated: false

  field :time_partitioning, 10,
    type: Google.Cloud.Bigquery.V2.TimePartitioning,
    json_name: "timePartitioning"

  field :range_partitioning, 27,
    type: Google.Cloud.Bigquery.V2.RangePartitioning,
    json_name: "rangePartitioning"

  field :clustering, 23, type: Google.Cloud.Bigquery.V2.Clustering

  field :require_partition_filter, 28,
    type: Google.Protobuf.BoolValue,
    json_name: "requirePartitionFilter",
    deprecated: false

  field :partition_definition, 51,
    proto3_optional: true,
    type: Google.Cloud.Bigquery.V2.PartitioningDefinition,
    json_name: "partitionDefinition",
    deprecated: false

  field :num_bytes, 11, type: Google.Protobuf.Int64Value, json_name: "numBytes", deprecated: false

  field :num_physical_bytes, 26,
    type: Google.Protobuf.Int64Value,
    json_name: "numPhysicalBytes",
    deprecated: false

  field :num_long_term_bytes, 12,
    type: Google.Protobuf.Int64Value,
    json_name: "numLongTermBytes",
    deprecated: false

  field :num_rows, 13, type: Google.Protobuf.UInt64Value, json_name: "numRows", deprecated: false
  field :creation_time, 14, type: :int64, json_name: "creationTime", deprecated: false

  field :expiration_time, 15,
    type: Google.Protobuf.Int64Value,
    json_name: "expirationTime",
    deprecated: false

  field :last_modified_time, 16, type: :fixed64, json_name: "lastModifiedTime", deprecated: false
  field :type, 17, type: :string, deprecated: false
  field :view, 18, type: Google.Cloud.Bigquery.V2.ViewDefinition, deprecated: false

  field :materialized_view, 25,
    type: Google.Cloud.Bigquery.V2.MaterializedViewDefinition,
    json_name: "materializedView",
    deprecated: false

  field :materialized_view_status, 42,
    type: Google.Cloud.Bigquery.V2.MaterializedViewStatus,
    json_name: "materializedViewStatus",
    deprecated: false

  field :external_data_configuration, 19,
    type: Google.Cloud.Bigquery.V2.ExternalDataConfiguration,
    json_name: "externalDataConfiguration",
    deprecated: false

  field :biglake_configuration, 45,
    type: Google.Cloud.Bigquery.V2.BigLakeConfiguration,
    json_name: "biglakeConfiguration",
    deprecated: false

  field :managed_table_type, 55,
    type: Google.Cloud.Bigquery.V2.ManagedTableType,
    json_name: "managedTableType",
    enum: true,
    deprecated: false

  field :location, 20, type: :string, deprecated: false

  field :streaming_buffer, 21,
    type: Google.Cloud.Bigquery.V2.Streamingbuffer,
    json_name: "streamingBuffer",
    deprecated: false

  field :encryption_configuration, 22,
    type: Google.Cloud.Bigquery.V2.EncryptionConfiguration,
    json_name: "encryptionConfiguration"

  field :snapshot_definition, 29,
    type: Google.Cloud.Bigquery.V2.SnapshotDefinition,
    json_name: "snapshotDefinition",
    deprecated: false

  field :default_collation, 30,
    type: Google.Protobuf.StringValue,
    json_name: "defaultCollation",
    deprecated: false

  field :default_rounding_mode, 44,
    type: Google.Cloud.Bigquery.V2.TableFieldSchema.RoundingMode,
    json_name: "defaultRoundingMode",
    enum: true,
    deprecated: false

  field :clone_definition, 31,
    type: Google.Cloud.Bigquery.V2.CloneDefinition,
    json_name: "cloneDefinition",
    deprecated: false

  field :num_time_travel_physical_bytes, 33,
    type: Google.Protobuf.Int64Value,
    json_name: "numTimeTravelPhysicalBytes",
    deprecated: false

  field :num_total_logical_bytes, 34,
    type: Google.Protobuf.Int64Value,
    json_name: "numTotalLogicalBytes",
    deprecated: false

  field :num_active_logical_bytes, 35,
    type: Google.Protobuf.Int64Value,
    json_name: "numActiveLogicalBytes",
    deprecated: false

  field :num_long_term_logical_bytes, 36,
    type: Google.Protobuf.Int64Value,
    json_name: "numLongTermLogicalBytes",
    deprecated: false

  field :num_current_physical_bytes, 53,
    type: Google.Protobuf.Int64Value,
    json_name: "numCurrentPhysicalBytes",
    deprecated: false

  field :num_total_physical_bytes, 37,
    type: Google.Protobuf.Int64Value,
    json_name: "numTotalPhysicalBytes",
    deprecated: false

  field :num_active_physical_bytes, 38,
    type: Google.Protobuf.Int64Value,
    json_name: "numActivePhysicalBytes",
    deprecated: false

  field :num_long_term_physical_bytes, 39,
    type: Google.Protobuf.Int64Value,
    json_name: "numLongTermPhysicalBytes",
    deprecated: false

  field :num_partitions, 40,
    type: Google.Protobuf.Int64Value,
    json_name: "numPartitions",
    deprecated: false

  field :max_staleness, 41, type: :string, json_name: "maxStaleness", deprecated: false
  field :restrictions, 46, type: Google.Cloud.Bigquery.V2.RestrictionConfig, deprecated: false

  field :table_constraints, 47,
    type: Google.Cloud.Bigquery.V2.TableConstraints,
    json_name: "tableConstraints",
    deprecated: false

  field :resource_tags, 48,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.Table.ResourceTagsEntry,
    json_name: "resourceTags",
    map: true,
    deprecated: false

  field :table_replication_info, 49,
    type: Google.Cloud.Bigquery.V2.TableReplicationInfo,
    json_name: "tableReplicationInfo",
    deprecated: false

  field :replicas, 50,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.TableReference,
    deprecated: false

  field :external_catalog_table_options, 54,
    type: Google.Cloud.Bigquery.V2.ExternalCatalogTableOptions,
    json_name: "externalCatalogTableOptions",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.GetTableRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId", deprecated: false
  field :dataset_id, 2, type: :string, json_name: "datasetId", deprecated: false
  field :table_id, 3, type: :string, json_name: "tableId", deprecated: false
  field :selected_fields, 4, type: :string, json_name: "selectedFields"

  field :view, 5,
    type: Google.Cloud.Bigquery.V2.GetTableRequest.TableMetadataView,
    enum: true,
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.InsertTableRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId", deprecated: false
  field :dataset_id, 2, type: :string, json_name: "datasetId", deprecated: false
  field :table, 4, type: Google.Cloud.Bigquery.V2.Table, deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.UpdateOrPatchTableRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId", deprecated: false
  field :dataset_id, 2, type: :string, json_name: "datasetId", deprecated: false
  field :table_id, 3, type: :string, json_name: "tableId", deprecated: false
  field :table, 4, type: Google.Cloud.Bigquery.V2.Table, deprecated: false
  field :autodetect_schema, 5, type: :bool, json_name: "autodetectSchema", deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.DeleteTableRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId", deprecated: false
  field :dataset_id, 2, type: :string, json_name: "datasetId", deprecated: false
  field :table_id, 3, type: :string, json_name: "tableId", deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.ListTablesRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId", deprecated: false
  field :dataset_id, 2, type: :string, json_name: "datasetId", deprecated: false
  field :max_results, 3, type: Google.Protobuf.UInt32Value, json_name: "maxResults"
  field :page_token, 4, type: :string, json_name: "pageToken"
end

defmodule Google.Cloud.Bigquery.V2.ListFormatView do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :use_legacy_sql, 1, type: Google.Protobuf.BoolValue, json_name: "useLegacySql"

  field :privacy_policy, 2,
    type: Google.Cloud.Bigquery.V2.PrivacyPolicy,
    json_name: "privacyPolicy"
end

defmodule Google.Cloud.Bigquery.V2.ListFormatTable.LabelsEntry do
  @moduledoc false

  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Google.Cloud.Bigquery.V2.ListFormatTable do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :kind, 1, type: :string
  field :id, 2, type: :string

  field :table_reference, 3,
    type: Google.Cloud.Bigquery.V2.TableReference,
    json_name: "tableReference"

  field :friendly_name, 4, type: Google.Protobuf.StringValue, json_name: "friendlyName"
  field :type, 5, type: :string

  field :time_partitioning, 6,
    type: Google.Cloud.Bigquery.V2.TimePartitioning,
    json_name: "timePartitioning"

  field :range_partitioning, 12,
    type: Google.Cloud.Bigquery.V2.RangePartitioning,
    json_name: "rangePartitioning"

  field :clustering, 11, type: Google.Cloud.Bigquery.V2.Clustering

  field :labels, 7,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.ListFormatTable.LabelsEntry,
    map: true

  field :view, 8, type: Google.Cloud.Bigquery.V2.ListFormatView
  field :creation_time, 9, type: :int64, json_name: "creationTime", deprecated: false
  field :expiration_time, 10, type: :int64, json_name: "expirationTime"

  field :require_partition_filter, 14,
    type: Google.Protobuf.BoolValue,
    json_name: "requirePartitionFilter",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.TableList do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :kind, 1, type: :string
  field :etag, 2, type: :string
  field :next_page_token, 3, type: :string, json_name: "nextPageToken"
  field :tables, 4, repeated: true, type: Google.Cloud.Bigquery.V2.ListFormatTable
  field :total_items, 5, type: Google.Protobuf.Int32Value, json_name: "totalItems"
end

defmodule Google.Cloud.Bigquery.V2.TableService.Service do
  @moduledoc false

  use GRPC.Service,
    name: "google.cloud.bigquery.v2.TableService",
    protoc_gen_elixir_version: "0.13.0"

  rpc(:GetTable, Google.Cloud.Bigquery.V2.GetTableRequest, Google.Cloud.Bigquery.V2.Table)

  rpc(:InsertTable, Google.Cloud.Bigquery.V2.InsertTableRequest, Google.Cloud.Bigquery.V2.Table)

  rpc(
    :PatchTable,
    Google.Cloud.Bigquery.V2.UpdateOrPatchTableRequest,
    Google.Cloud.Bigquery.V2.Table
  )

  rpc(
    :UpdateTable,
    Google.Cloud.Bigquery.V2.UpdateOrPatchTableRequest,
    Google.Cloud.Bigquery.V2.Table
  )

  rpc(:DeleteTable, Google.Cloud.Bigquery.V2.DeleteTableRequest, Google.Protobuf.Empty)

  rpc(:ListTables, Google.Cloud.Bigquery.V2.ListTablesRequest, Google.Cloud.Bigquery.V2.TableList)
end

defmodule Google.Cloud.Bigquery.V2.TableService.Stub do
  @moduledoc false

  use GRPC.Stub, service: Google.Cloud.Bigquery.V2.TableService.Service
end
