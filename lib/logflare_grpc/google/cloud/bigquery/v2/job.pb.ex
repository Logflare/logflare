defmodule Google.Cloud.Bigquery.V2.ListJobsRequest.Projection do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :minimal, 0
  field :MINIMAL, 0
  field :full, 1
  field :FULL, 1
end

defmodule Google.Cloud.Bigquery.V2.ListJobsRequest.StateFilter do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :done, 0
  field :DONE, 0
  field :pending, 1
  field :PENDING, 1
  field :running, 2
  field :RUNNING, 2
end

defmodule Google.Cloud.Bigquery.V2.QueryRequest.JobCreationMode do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :JOB_CREATION_MODE_UNSPECIFIED, 0
  field :JOB_CREATION_REQUIRED, 1
  field :JOB_CREATION_OPTIONAL, 2
end

defmodule Google.Cloud.Bigquery.V2.Job do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :kind, 1, type: :string, deprecated: false
  field :etag, 2, type: :string, deprecated: false
  field :id, 3, type: :string, deprecated: false
  field :self_link, 4, type: :string, json_name: "selfLink", deprecated: false
  field :user_email, 5, type: :string, deprecated: false
  field :configuration, 6, type: Google.Cloud.Bigquery.V2.JobConfiguration, deprecated: false

  field :job_reference, 7,
    type: Google.Cloud.Bigquery.V2.JobReference,
    json_name: "jobReference",
    deprecated: false

  field :statistics, 8, type: Google.Cloud.Bigquery.V2.JobStatistics, deprecated: false
  field :status, 9, type: Google.Cloud.Bigquery.V2.JobStatus, deprecated: false
  field :principal_subject, 13, type: :string, deprecated: false

  field :job_creation_reason, 14,
    type: Google.Cloud.Bigquery.V2.JobCreationReason,
    json_name: "jobCreationReason",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.CancelJobRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId", deprecated: false
  field :job_id, 2, type: :string, json_name: "jobId", deprecated: false
  field :location, 3, type: :string
end

defmodule Google.Cloud.Bigquery.V2.JobCancelResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :kind, 1, type: :string
  field :job, 2, type: Google.Cloud.Bigquery.V2.Job
end

defmodule Google.Cloud.Bigquery.V2.GetJobRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId", deprecated: false
  field :job_id, 2, type: :string, json_name: "jobId", deprecated: false
  field :location, 3, type: :string
end

defmodule Google.Cloud.Bigquery.V2.InsertJobRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId"
  field :job, 3, type: Google.Cloud.Bigquery.V2.Job
end

defmodule Google.Cloud.Bigquery.V2.DeleteJobRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId", deprecated: false
  field :job_id, 2, type: :string, json_name: "jobId", deprecated: false
  field :location, 3, type: :string
end

defmodule Google.Cloud.Bigquery.V2.ListJobsRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId"
  field :all_users, 2, type: :bool, json_name: "allUsers"
  field :max_results, 3, type: Google.Protobuf.Int32Value, json_name: "maxResults"
  field :min_creation_time, 4, type: :uint64, json_name: "minCreationTime"
  field :max_creation_time, 5, type: Google.Protobuf.UInt64Value, json_name: "maxCreationTime"
  field :page_token, 6, type: :string, json_name: "pageToken"
  field :projection, 7, type: Google.Cloud.Bigquery.V2.ListJobsRequest.Projection, enum: true

  field :state_filter, 8,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.ListJobsRequest.StateFilter,
    json_name: "stateFilter",
    enum: true

  field :parent_job_id, 9, type: :string, json_name: "parentJobId"
end

defmodule Google.Cloud.Bigquery.V2.ListFormatJob do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :id, 1, type: :string
  field :kind, 2, type: :string
  field :job_reference, 3, type: Google.Cloud.Bigquery.V2.JobReference, json_name: "jobReference"
  field :state, 4, type: :string
  field :error_result, 5, type: Google.Cloud.Bigquery.V2.ErrorProto, json_name: "errorResult"
  field :statistics, 6, type: Google.Cloud.Bigquery.V2.JobStatistics, deprecated: false
  field :configuration, 7, type: Google.Cloud.Bigquery.V2.JobConfiguration, deprecated: false
  field :status, 8, type: Google.Cloud.Bigquery.V2.JobStatus
  field :user_email, 9, type: :string
  field :principal_subject, 10, type: :string
end

defmodule Google.Cloud.Bigquery.V2.JobList do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :etag, 1, type: :string
  field :kind, 2, type: :string
  field :next_page_token, 3, type: :string, json_name: "nextPageToken"
  field :jobs, 4, repeated: true, type: Google.Cloud.Bigquery.V2.ListFormatJob
  field :unreachable, 5, repeated: true, type: :string
end

defmodule Google.Cloud.Bigquery.V2.GetQueryResultsRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId", deprecated: false
  field :job_id, 2, type: :string, json_name: "jobId", deprecated: false
  field :start_index, 3, type: Google.Protobuf.UInt64Value, json_name: "startIndex"
  field :page_token, 4, type: :string, json_name: "pageToken"
  field :max_results, 5, type: Google.Protobuf.UInt32Value, json_name: "maxResults"
  field :timeout_ms, 6, type: Google.Protobuf.UInt32Value, json_name: "timeoutMs"
  field :location, 7, type: :string

  field :format_options, 8,
    type: Google.Cloud.Bigquery.V2.DataFormatOptions,
    json_name: "formatOptions",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.GetQueryResultsResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :kind, 1, type: :string
  field :etag, 2, type: :string
  field :schema, 3, type: Google.Cloud.Bigquery.V2.TableSchema
  field :job_reference, 4, type: Google.Cloud.Bigquery.V2.JobReference, json_name: "jobReference"
  field :total_rows, 5, type: Google.Protobuf.UInt64Value, json_name: "totalRows"
  field :page_token, 6, type: :string, json_name: "pageToken"
  field :rows, 7, repeated: true, type: Google.Protobuf.Struct

  field :total_bytes_processed, 8,
    type: Google.Protobuf.Int64Value,
    json_name: "totalBytesProcessed"

  field :job_complete, 9, type: Google.Protobuf.BoolValue, json_name: "jobComplete"
  field :errors, 10, repeated: true, type: Google.Cloud.Bigquery.V2.ErrorProto, deprecated: false
  field :cache_hit, 11, type: Google.Protobuf.BoolValue, json_name: "cacheHit"

  field :num_dml_affected_rows, 12,
    type: Google.Protobuf.Int64Value,
    json_name: "numDmlAffectedRows",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.PostQueryRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId", deprecated: false
  field :query_request, 2, type: Google.Cloud.Bigquery.V2.QueryRequest, json_name: "queryRequest"
end

defmodule Google.Cloud.Bigquery.V2.QueryRequest.LabelsEntry do
  @moduledoc false

  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Google.Cloud.Bigquery.V2.QueryRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :kind, 2, type: :string
  field :query, 3, type: :string, deprecated: false

  field :max_results, 4,
    type: Google.Protobuf.UInt32Value,
    json_name: "maxResults",
    deprecated: false

  field :default_dataset, 5,
    type: Google.Cloud.Bigquery.V2.DatasetReference,
    json_name: "defaultDataset",
    deprecated: false

  field :timeout_ms, 6,
    type: Google.Protobuf.UInt32Value,
    json_name: "timeoutMs",
    deprecated: false

  field :job_timeout_ms, 26,
    proto3_optional: true,
    type: :int64,
    json_name: "jobTimeoutMs",
    deprecated: false

  field :destination_encryption_configuration, 27,
    type: Google.Cloud.Bigquery.V2.EncryptionConfiguration,
    json_name: "destinationEncryptionConfiguration",
    deprecated: false

  field :dry_run, 7, type: :bool, json_name: "dryRun", deprecated: false

  field :use_query_cache, 9,
    type: Google.Protobuf.BoolValue,
    json_name: "useQueryCache",
    deprecated: false

  field :use_legacy_sql, 10, type: Google.Protobuf.BoolValue, json_name: "useLegacySql"
  field :parameter_mode, 11, type: :string, json_name: "parameterMode"

  field :query_parameters, 12,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.QueryParameter,
    json_name: "queryParameters"

  field :location, 13, type: :string

  field :format_options, 15,
    type: Google.Cloud.Bigquery.V2.DataFormatOptions,
    json_name: "formatOptions",
    deprecated: false

  field :connection_properties, 16,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.ConnectionProperty,
    json_name: "connectionProperties",
    deprecated: false

  field :labels, 17,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.QueryRequest.LabelsEntry,
    map: true,
    deprecated: false

  field :maximum_bytes_billed, 18,
    type: Google.Protobuf.Int64Value,
    json_name: "maximumBytesBilled",
    deprecated: false

  field :request_id, 19, type: :string, json_name: "requestId", deprecated: false

  field :create_session, 20,
    type: Google.Protobuf.BoolValue,
    json_name: "createSession",
    deprecated: false

  field :job_creation_mode, 22,
    type: Google.Cloud.Bigquery.V2.QueryRequest.JobCreationMode,
    json_name: "jobCreationMode",
    enum: true,
    deprecated: false

  field :reservation, 24, proto3_optional: true, type: :string, deprecated: false

  field :write_incremental_results, 25,
    type: :bool,
    json_name: "writeIncrementalResults",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.QueryResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :kind, 1, type: :string
  field :schema, 2, type: Google.Cloud.Bigquery.V2.TableSchema
  field :job_reference, 3, type: Google.Cloud.Bigquery.V2.JobReference, json_name: "jobReference"

  field :job_creation_reason, 15,
    type: Google.Cloud.Bigquery.V2.JobCreationReason,
    json_name: "jobCreationReason",
    deprecated: false

  field :query_id, 14, type: :string, json_name: "queryId"
  field :location, 18, type: :string, deprecated: false
  field :total_rows, 4, type: Google.Protobuf.UInt64Value, json_name: "totalRows"
  field :page_token, 5, type: :string, json_name: "pageToken"
  field :rows, 6, repeated: true, type: Google.Protobuf.Struct

  field :total_bytes_processed, 7,
    type: Google.Protobuf.Int64Value,
    json_name: "totalBytesProcessed"

  field :total_bytes_billed, 16,
    proto3_optional: true,
    type: :int64,
    json_name: "totalBytesBilled",
    deprecated: false

  field :total_slot_ms, 17,
    proto3_optional: true,
    type: :int64,
    json_name: "totalSlotMs",
    deprecated: false

  field :job_complete, 8, type: Google.Protobuf.BoolValue, json_name: "jobComplete"
  field :errors, 9, repeated: true, type: Google.Cloud.Bigquery.V2.ErrorProto, deprecated: false
  field :cache_hit, 10, type: Google.Protobuf.BoolValue, json_name: "cacheHit"

  field :num_dml_affected_rows, 11,
    type: Google.Protobuf.Int64Value,
    json_name: "numDmlAffectedRows",
    deprecated: false

  field :session_info, 12,
    type: Google.Cloud.Bigquery.V2.SessionInfo,
    json_name: "sessionInfo",
    deprecated: false

  field :dml_stats, 13,
    type: Google.Cloud.Bigquery.V2.DmlStats,
    json_name: "dmlStats",
    deprecated: false

  field :creation_time, 19,
    proto3_optional: true,
    type: :int64,
    json_name: "creationTime",
    deprecated: false

  field :start_time, 20,
    proto3_optional: true,
    type: :int64,
    json_name: "startTime",
    deprecated: false

  field :end_time, 21,
    proto3_optional: true,
    type: :int64,
    json_name: "endTime",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.JobService.Service do
  @moduledoc false

  use GRPC.Service,
    name: "google.cloud.bigquery.v2.JobService",
    protoc_gen_elixir_version: "0.13.0"

  rpc(
    :CancelJob,
    Google.Cloud.Bigquery.V2.CancelJobRequest,
    Google.Cloud.Bigquery.V2.JobCancelResponse
  )

  rpc(:GetJob, Google.Cloud.Bigquery.V2.GetJobRequest, Google.Cloud.Bigquery.V2.Job)

  rpc(:InsertJob, Google.Cloud.Bigquery.V2.InsertJobRequest, Google.Cloud.Bigquery.V2.Job)

  rpc(:DeleteJob, Google.Cloud.Bigquery.V2.DeleteJobRequest, Google.Protobuf.Empty)

  rpc(:ListJobs, Google.Cloud.Bigquery.V2.ListJobsRequest, Google.Cloud.Bigquery.V2.JobList)

  rpc(
    :GetQueryResults,
    Google.Cloud.Bigquery.V2.GetQueryResultsRequest,
    Google.Cloud.Bigquery.V2.GetQueryResultsResponse
  )

  rpc(:Query, Google.Cloud.Bigquery.V2.PostQueryRequest, Google.Cloud.Bigquery.V2.QueryResponse)
end

defmodule Google.Cloud.Bigquery.V2.JobService.Stub do
  @moduledoc false

  use GRPC.Stub, service: Google.Cloud.Bigquery.V2.JobService.Service
end
