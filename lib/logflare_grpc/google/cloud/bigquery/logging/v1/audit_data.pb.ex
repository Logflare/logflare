defmodule Google.Cloud.Bigquery.Logging.V1.AuditData do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:request, 0)

  oneof(:response, 1)

  field :table_insert_request, 1,
    type: Google.Cloud.Bigquery.Logging.V1.TableInsertRequest,
    json_name: "tableInsertRequest",
    oneof: 0

  field :table_update_request, 16,
    type: Google.Cloud.Bigquery.Logging.V1.TableUpdateRequest,
    json_name: "tableUpdateRequest",
    oneof: 0

  field :dataset_list_request, 2,
    type: Google.Cloud.Bigquery.Logging.V1.DatasetListRequest,
    json_name: "datasetListRequest",
    oneof: 0

  field :dataset_insert_request, 3,
    type: Google.Cloud.Bigquery.Logging.V1.DatasetInsertRequest,
    json_name: "datasetInsertRequest",
    oneof: 0

  field :dataset_update_request, 4,
    type: Google.Cloud.Bigquery.Logging.V1.DatasetUpdateRequest,
    json_name: "datasetUpdateRequest",
    oneof: 0

  field :job_insert_request, 5,
    type: Google.Cloud.Bigquery.Logging.V1.JobInsertRequest,
    json_name: "jobInsertRequest",
    oneof: 0

  field :job_query_request, 6,
    type: Google.Cloud.Bigquery.Logging.V1.JobQueryRequest,
    json_name: "jobQueryRequest",
    oneof: 0

  field :job_get_query_results_request, 7,
    type: Google.Cloud.Bigquery.Logging.V1.JobGetQueryResultsRequest,
    json_name: "jobGetQueryResultsRequest",
    oneof: 0

  field :table_data_list_request, 8,
    type: Google.Cloud.Bigquery.Logging.V1.TableDataListRequest,
    json_name: "tableDataListRequest",
    oneof: 0

  field :set_iam_policy_request, 20,
    type: Google.Iam.V1.SetIamPolicyRequest,
    json_name: "setIamPolicyRequest",
    oneof: 0

  field :table_insert_response, 9,
    type: Google.Cloud.Bigquery.Logging.V1.TableInsertResponse,
    json_name: "tableInsertResponse",
    oneof: 1

  field :table_update_response, 10,
    type: Google.Cloud.Bigquery.Logging.V1.TableUpdateResponse,
    json_name: "tableUpdateResponse",
    oneof: 1

  field :dataset_insert_response, 11,
    type: Google.Cloud.Bigquery.Logging.V1.DatasetInsertResponse,
    json_name: "datasetInsertResponse",
    oneof: 1

  field :dataset_update_response, 12,
    type: Google.Cloud.Bigquery.Logging.V1.DatasetUpdateResponse,
    json_name: "datasetUpdateResponse",
    oneof: 1

  field :job_insert_response, 18,
    type: Google.Cloud.Bigquery.Logging.V1.JobInsertResponse,
    json_name: "jobInsertResponse",
    oneof: 1

  field :job_query_response, 13,
    type: Google.Cloud.Bigquery.Logging.V1.JobQueryResponse,
    json_name: "jobQueryResponse",
    oneof: 1

  field :job_get_query_results_response, 14,
    type: Google.Cloud.Bigquery.Logging.V1.JobGetQueryResultsResponse,
    json_name: "jobGetQueryResultsResponse",
    oneof: 1

  field :job_query_done_response, 15,
    type: Google.Cloud.Bigquery.Logging.V1.JobQueryDoneResponse,
    json_name: "jobQueryDoneResponse",
    oneof: 1

  field :policy_response, 21, type: Google.Iam.V1.Policy, json_name: "policyResponse", oneof: 1

  field :job_completed_event, 17,
    type: Google.Cloud.Bigquery.Logging.V1.JobCompletedEvent,
    json_name: "jobCompletedEvent"

  field :table_data_read_events, 19,
    repeated: true,
    type: Google.Cloud.Bigquery.Logging.V1.TableDataReadEvent,
    json_name: "tableDataReadEvents"
end

defmodule Google.Cloud.Bigquery.Logging.V1.TableInsertRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :resource, 1, type: Google.Cloud.Bigquery.Logging.V1.Table
end

defmodule Google.Cloud.Bigquery.Logging.V1.TableUpdateRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :resource, 1, type: Google.Cloud.Bigquery.Logging.V1.Table
end

defmodule Google.Cloud.Bigquery.Logging.V1.TableInsertResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :resource, 1, type: Google.Cloud.Bigquery.Logging.V1.Table
end

defmodule Google.Cloud.Bigquery.Logging.V1.TableUpdateResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :resource, 1, type: Google.Cloud.Bigquery.Logging.V1.Table
end

defmodule Google.Cloud.Bigquery.Logging.V1.DatasetListRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :list_all, 1, type: :bool, json_name: "listAll"
end

defmodule Google.Cloud.Bigquery.Logging.V1.DatasetInsertRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :resource, 1, type: Google.Cloud.Bigquery.Logging.V1.Dataset
end

defmodule Google.Cloud.Bigquery.Logging.V1.DatasetInsertResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :resource, 1, type: Google.Cloud.Bigquery.Logging.V1.Dataset
end

defmodule Google.Cloud.Bigquery.Logging.V1.DatasetUpdateRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :resource, 1, type: Google.Cloud.Bigquery.Logging.V1.Dataset
end

defmodule Google.Cloud.Bigquery.Logging.V1.DatasetUpdateResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :resource, 1, type: Google.Cloud.Bigquery.Logging.V1.Dataset
end

defmodule Google.Cloud.Bigquery.Logging.V1.JobInsertRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :resource, 1, type: Google.Cloud.Bigquery.Logging.V1.Job
end

defmodule Google.Cloud.Bigquery.Logging.V1.JobInsertResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :resource, 1, type: Google.Cloud.Bigquery.Logging.V1.Job
end

defmodule Google.Cloud.Bigquery.Logging.V1.JobQueryRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :query, 1, type: :string
  field :max_results, 2, type: :uint32, json_name: "maxResults"

  field :default_dataset, 3,
    type: Google.Cloud.Bigquery.Logging.V1.DatasetName,
    json_name: "defaultDataset"

  field :project_id, 4, type: :string, json_name: "projectId"
  field :dry_run, 5, type: :bool, json_name: "dryRun"
end

defmodule Google.Cloud.Bigquery.Logging.V1.JobQueryResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :total_results, 1, type: :uint64, json_name: "totalResults"
  field :job, 2, type: Google.Cloud.Bigquery.Logging.V1.Job
end

defmodule Google.Cloud.Bigquery.Logging.V1.JobGetQueryResultsRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :max_results, 1, type: :uint32, json_name: "maxResults"
  field :start_row, 2, type: :uint64, json_name: "startRow"
end

defmodule Google.Cloud.Bigquery.Logging.V1.JobGetQueryResultsResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :total_results, 1, type: :uint64, json_name: "totalResults"
  field :job, 2, type: Google.Cloud.Bigquery.Logging.V1.Job
end

defmodule Google.Cloud.Bigquery.Logging.V1.JobQueryDoneResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :job, 1, type: Google.Cloud.Bigquery.Logging.V1.Job
end

defmodule Google.Cloud.Bigquery.Logging.V1.JobCompletedEvent do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :event_name, 1, type: :string, json_name: "eventName"
  field :job, 2, type: Google.Cloud.Bigquery.Logging.V1.Job
end

defmodule Google.Cloud.Bigquery.Logging.V1.TableDataReadEvent do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :table_name, 1, type: Google.Cloud.Bigquery.Logging.V1.TableName, json_name: "tableName"
  field :referenced_fields, 2, repeated: true, type: :string, json_name: "referencedFields"
end

defmodule Google.Cloud.Bigquery.Logging.V1.TableDataListRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :start_row, 1, type: :uint64, json_name: "startRow"
  field :max_results, 2, type: :uint32, json_name: "maxResults"
end

defmodule Google.Cloud.Bigquery.Logging.V1.Table do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :table_name, 1, type: Google.Cloud.Bigquery.Logging.V1.TableName, json_name: "tableName"
  field :info, 2, type: Google.Cloud.Bigquery.Logging.V1.TableInfo
  field :schema_json, 8, type: :string, json_name: "schemaJson"
  field :view, 4, type: Google.Cloud.Bigquery.Logging.V1.TableViewDefinition
  field :expire_time, 5, type: Google.Protobuf.Timestamp, json_name: "expireTime"
  field :create_time, 6, type: Google.Protobuf.Timestamp, json_name: "createTime"
  field :truncate_time, 7, type: Google.Protobuf.Timestamp, json_name: "truncateTime"
  field :update_time, 9, type: Google.Protobuf.Timestamp, json_name: "updateTime"
  field :encryption, 10, type: Google.Cloud.Bigquery.Logging.V1.EncryptionInfo
end

defmodule Google.Cloud.Bigquery.Logging.V1.TableInfo.LabelsEntry do
  @moduledoc false

  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Google.Cloud.Bigquery.Logging.V1.TableInfo do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :friendly_name, 1, type: :string, json_name: "friendlyName"
  field :description, 2, type: :string

  field :labels, 3,
    repeated: true,
    type: Google.Cloud.Bigquery.Logging.V1.TableInfo.LabelsEntry,
    map: true
end

defmodule Google.Cloud.Bigquery.Logging.V1.TableViewDefinition do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :query, 1, type: :string
end

defmodule Google.Cloud.Bigquery.Logging.V1.Dataset do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :dataset_name, 1,
    type: Google.Cloud.Bigquery.Logging.V1.DatasetName,
    json_name: "datasetName"

  field :info, 2, type: Google.Cloud.Bigquery.Logging.V1.DatasetInfo
  field :create_time, 4, type: Google.Protobuf.Timestamp, json_name: "createTime"
  field :update_time, 5, type: Google.Protobuf.Timestamp, json_name: "updateTime"
  field :acl, 6, type: Google.Cloud.Bigquery.Logging.V1.BigQueryAcl

  field :default_table_expire_duration, 8,
    type: Google.Protobuf.Duration,
    json_name: "defaultTableExpireDuration"
end

defmodule Google.Cloud.Bigquery.Logging.V1.DatasetInfo.LabelsEntry do
  @moduledoc false

  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Google.Cloud.Bigquery.Logging.V1.DatasetInfo do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :friendly_name, 1, type: :string, json_name: "friendlyName"
  field :description, 2, type: :string

  field :labels, 3,
    repeated: true,
    type: Google.Cloud.Bigquery.Logging.V1.DatasetInfo.LabelsEntry,
    map: true
end

defmodule Google.Cloud.Bigquery.Logging.V1.BigQueryAcl.Entry do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :role, 1, type: :string
  field :group_email, 2, type: :string, json_name: "groupEmail"
  field :user_email, 3, type: :string, json_name: "userEmail"
  field :domain, 4, type: :string
  field :special_group, 5, type: :string, json_name: "specialGroup"
  field :view_name, 6, type: Google.Cloud.Bigquery.Logging.V1.TableName, json_name: "viewName"
end

defmodule Google.Cloud.Bigquery.Logging.V1.BigQueryAcl do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :entries, 1, repeated: true, type: Google.Cloud.Bigquery.Logging.V1.BigQueryAcl.Entry
end

defmodule Google.Cloud.Bigquery.Logging.V1.Job do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :job_name, 1, type: Google.Cloud.Bigquery.Logging.V1.JobName, json_name: "jobName"

  field :job_configuration, 2,
    type: Google.Cloud.Bigquery.Logging.V1.JobConfiguration,
    json_name: "jobConfiguration"

  field :job_status, 3, type: Google.Cloud.Bigquery.Logging.V1.JobStatus, json_name: "jobStatus"

  field :job_statistics, 4,
    type: Google.Cloud.Bigquery.Logging.V1.JobStatistics,
    json_name: "jobStatistics"
end

defmodule Google.Cloud.Bigquery.Logging.V1.JobConfiguration.Query do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :query, 1, type: :string

  field :destination_table, 2,
    type: Google.Cloud.Bigquery.Logging.V1.TableName,
    json_name: "destinationTable"

  field :create_disposition, 3, type: :string, json_name: "createDisposition"
  field :write_disposition, 4, type: :string, json_name: "writeDisposition"

  field :default_dataset, 5,
    type: Google.Cloud.Bigquery.Logging.V1.DatasetName,
    json_name: "defaultDataset"

  field :table_definitions, 6,
    repeated: true,
    type: Google.Cloud.Bigquery.Logging.V1.TableDefinition,
    json_name: "tableDefinitions"

  field :query_priority, 7, type: :string, json_name: "queryPriority"

  field :destination_table_encryption, 8,
    type: Google.Cloud.Bigquery.Logging.V1.EncryptionInfo,
    json_name: "destinationTableEncryption"

  field :statement_type, 9, type: :string, json_name: "statementType"
end

defmodule Google.Cloud.Bigquery.Logging.V1.JobConfiguration.Load do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :source_uris, 1, repeated: true, type: :string, json_name: "sourceUris"
  field :schema_json, 6, type: :string, json_name: "schemaJson"

  field :destination_table, 3,
    type: Google.Cloud.Bigquery.Logging.V1.TableName,
    json_name: "destinationTable"

  field :create_disposition, 4, type: :string, json_name: "createDisposition"
  field :write_disposition, 5, type: :string, json_name: "writeDisposition"

  field :destination_table_encryption, 7,
    type: Google.Cloud.Bigquery.Logging.V1.EncryptionInfo,
    json_name: "destinationTableEncryption"
end

defmodule Google.Cloud.Bigquery.Logging.V1.JobConfiguration.Extract do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :destination_uris, 1, repeated: true, type: :string, json_name: "destinationUris"

  field :source_table, 2,
    type: Google.Cloud.Bigquery.Logging.V1.TableName,
    json_name: "sourceTable"
end

defmodule Google.Cloud.Bigquery.Logging.V1.JobConfiguration.TableCopy do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :source_tables, 1,
    repeated: true,
    type: Google.Cloud.Bigquery.Logging.V1.TableName,
    json_name: "sourceTables"

  field :destination_table, 2,
    type: Google.Cloud.Bigquery.Logging.V1.TableName,
    json_name: "destinationTable"

  field :create_disposition, 3, type: :string, json_name: "createDisposition"
  field :write_disposition, 4, type: :string, json_name: "writeDisposition"

  field :destination_table_encryption, 5,
    type: Google.Cloud.Bigquery.Logging.V1.EncryptionInfo,
    json_name: "destinationTableEncryption"
end

defmodule Google.Cloud.Bigquery.Logging.V1.JobConfiguration.LabelsEntry do
  @moduledoc false

  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Google.Cloud.Bigquery.Logging.V1.JobConfiguration do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:configuration, 0)

  field :query, 5, type: Google.Cloud.Bigquery.Logging.V1.JobConfiguration.Query, oneof: 0
  field :load, 6, type: Google.Cloud.Bigquery.Logging.V1.JobConfiguration.Load, oneof: 0
  field :extract, 7, type: Google.Cloud.Bigquery.Logging.V1.JobConfiguration.Extract, oneof: 0

  field :table_copy, 8,
    type: Google.Cloud.Bigquery.Logging.V1.JobConfiguration.TableCopy,
    json_name: "tableCopy",
    oneof: 0

  field :dry_run, 9, type: :bool, json_name: "dryRun"

  field :labels, 3,
    repeated: true,
    type: Google.Cloud.Bigquery.Logging.V1.JobConfiguration.LabelsEntry,
    map: true
end

defmodule Google.Cloud.Bigquery.Logging.V1.TableDefinition do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string
  field :source_uris, 2, repeated: true, type: :string, json_name: "sourceUris"
end

defmodule Google.Cloud.Bigquery.Logging.V1.JobStatus do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :state, 1, type: :string
  field :error, 2, type: Google.Rpc.Status

  field :additional_errors, 3,
    repeated: true,
    type: Google.Rpc.Status,
    json_name: "additionalErrors"
end

defmodule Google.Cloud.Bigquery.Logging.V1.JobStatistics.ReservationResourceUsage do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string
  field :slot_ms, 2, type: :int64, json_name: "slotMs"
end

defmodule Google.Cloud.Bigquery.Logging.V1.JobStatistics do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :create_time, 1, type: Google.Protobuf.Timestamp, json_name: "createTime"
  field :start_time, 2, type: Google.Protobuf.Timestamp, json_name: "startTime"
  field :end_time, 3, type: Google.Protobuf.Timestamp, json_name: "endTime"
  field :total_processed_bytes, 4, type: :int64, json_name: "totalProcessedBytes"
  field :total_billed_bytes, 5, type: :int64, json_name: "totalBilledBytes"
  field :billing_tier, 7, type: :int32, json_name: "billingTier"
  field :total_slot_ms, 8, type: :int64, json_name: "totalSlotMs"

  field :reservation_usage, 14,
    repeated: true,
    type: Google.Cloud.Bigquery.Logging.V1.JobStatistics.ReservationResourceUsage,
    json_name: "reservationUsage",
    deprecated: true

  field :reservation, 16, type: :string

  field :referenced_tables, 9,
    repeated: true,
    type: Google.Cloud.Bigquery.Logging.V1.TableName,
    json_name: "referencedTables"

  field :total_tables_processed, 10, type: :int32, json_name: "totalTablesProcessed"

  field :referenced_views, 11,
    repeated: true,
    type: Google.Cloud.Bigquery.Logging.V1.TableName,
    json_name: "referencedViews"

  field :total_views_processed, 12, type: :int32, json_name: "totalViewsProcessed"
  field :query_output_row_count, 15, type: :int64, json_name: "queryOutputRowCount"
  field :total_load_output_bytes, 13, type: :int64, json_name: "totalLoadOutputBytes"
end

defmodule Google.Cloud.Bigquery.Logging.V1.DatasetName do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId"
  field :dataset_id, 2, type: :string, json_name: "datasetId"
end

defmodule Google.Cloud.Bigquery.Logging.V1.TableName do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId"
  field :dataset_id, 2, type: :string, json_name: "datasetId"
  field :table_id, 3, type: :string, json_name: "tableId"
end

defmodule Google.Cloud.Bigquery.Logging.V1.JobName do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId"
  field :job_id, 2, type: :string, json_name: "jobId"
  field :location, 3, type: :string
end

defmodule Google.Cloud.Bigquery.Logging.V1.EncryptionInfo do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :kms_key_name, 1, type: :string, json_name: "kmsKeyName"
end
