defmodule Google.Cloud.Bigquery.Datatransfer.V1.DataSourceParameter.Type do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :TYPE_UNSPECIFIED, 0
  field :STRING, 1
  field :INTEGER, 2
  field :DOUBLE, 3
  field :BOOLEAN, 4
  field :RECORD, 5
  field :PLUS_PAGE, 6
  field :LIST, 7
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.DataSource.AuthorizationType do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :AUTHORIZATION_TYPE_UNSPECIFIED, 0
  field :AUTHORIZATION_CODE, 1
  field :GOOGLE_PLUS_AUTHORIZATION_CODE, 2
  field :FIRST_PARTY_OAUTH, 3
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.DataSource.DataRefreshType do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :DATA_REFRESH_TYPE_UNSPECIFIED, 0
  field :SLIDING_WINDOW, 1
  field :CUSTOM_SLIDING_WINDOW, 2
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.ListTransferRunsRequest.RunAttempt do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :RUN_ATTEMPT_UNSPECIFIED, 0
  field :LATEST, 1
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.DataSourceParameter do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :param_id, 1, type: :string, json_name: "paramId"
  field :display_name, 2, type: :string, json_name: "displayName"
  field :description, 3, type: :string
  field :type, 4, type: Google.Cloud.Bigquery.Datatransfer.V1.DataSourceParameter.Type, enum: true
  field :required, 5, type: :bool
  field :repeated, 6, type: :bool
  field :validation_regex, 7, type: :string, json_name: "validationRegex"
  field :allowed_values, 8, repeated: true, type: :string, json_name: "allowedValues"
  field :min_value, 9, type: Google.Protobuf.DoubleValue, json_name: "minValue"
  field :max_value, 10, type: Google.Protobuf.DoubleValue, json_name: "maxValue"

  field :fields, 11,
    repeated: true,
    type: Google.Cloud.Bigquery.Datatransfer.V1.DataSourceParameter

  field :validation_description, 12, type: :string, json_name: "validationDescription"
  field :validation_help_url, 13, type: :string, json_name: "validationHelpUrl"
  field :immutable, 14, type: :bool
  field :recurse, 15, type: :bool
  field :deprecated, 20, type: :bool
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.DataSource do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
  field :data_source_id, 2, type: :string, json_name: "dataSourceId"
  field :display_name, 3, type: :string, json_name: "displayName"
  field :description, 4, type: :string
  field :client_id, 5, type: :string, json_name: "clientId"
  field :scopes, 6, repeated: true, type: :string

  field :transfer_type, 7,
    type: Google.Cloud.Bigquery.Datatransfer.V1.TransferType,
    json_name: "transferType",
    enum: true,
    deprecated: true

  field :supports_multiple_transfers, 8,
    type: :bool,
    json_name: "supportsMultipleTransfers",
    deprecated: true

  field :update_deadline_seconds, 9, type: :int32, json_name: "updateDeadlineSeconds"
  field :default_schedule, 10, type: :string, json_name: "defaultSchedule"
  field :supports_custom_schedule, 11, type: :bool, json_name: "supportsCustomSchedule"

  field :parameters, 12,
    repeated: true,
    type: Google.Cloud.Bigquery.Datatransfer.V1.DataSourceParameter

  field :help_url, 13, type: :string, json_name: "helpUrl"

  field :authorization_type, 14,
    type: Google.Cloud.Bigquery.Datatransfer.V1.DataSource.AuthorizationType,
    json_name: "authorizationType",
    enum: true

  field :data_refresh_type, 15,
    type: Google.Cloud.Bigquery.Datatransfer.V1.DataSource.DataRefreshType,
    json_name: "dataRefreshType",
    enum: true

  field :default_data_refresh_window_days, 16,
    type: :int32,
    json_name: "defaultDataRefreshWindowDays"

  field :manual_runs_disabled, 17, type: :bool, json_name: "manualRunsDisabled"

  field :minimum_schedule_interval, 18,
    type: Google.Protobuf.Duration,
    json_name: "minimumScheduleInterval"
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.GetDataSourceRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.ListDataSourcesRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false
  field :page_token, 3, type: :string, json_name: "pageToken"
  field :page_size, 4, type: :int32, json_name: "pageSize"
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.ListDataSourcesResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :data_sources, 1,
    repeated: true,
    type: Google.Cloud.Bigquery.Datatransfer.V1.DataSource,
    json_name: "dataSources"

  field :next_page_token, 2, type: :string, json_name: "nextPageToken", deprecated: false
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.CreateTransferConfigRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false

  field :transfer_config, 2,
    type: Google.Cloud.Bigquery.Datatransfer.V1.TransferConfig,
    json_name: "transferConfig",
    deprecated: false

  field :authorization_code, 3, type: :string, json_name: "authorizationCode", deprecated: true
  field :version_info, 5, type: :string, json_name: "versionInfo"
  field :service_account_name, 6, type: :string, json_name: "serviceAccountName"
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.UpdateTransferConfigRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :transfer_config, 1,
    type: Google.Cloud.Bigquery.Datatransfer.V1.TransferConfig,
    json_name: "transferConfig",
    deprecated: false

  field :authorization_code, 3, type: :string, json_name: "authorizationCode", deprecated: true

  field :update_mask, 4,
    type: Google.Protobuf.FieldMask,
    json_name: "updateMask",
    deprecated: false

  field :version_info, 5, type: :string, json_name: "versionInfo"
  field :service_account_name, 6, type: :string, json_name: "serviceAccountName"
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.GetTransferConfigRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.DeleteTransferConfigRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.GetTransferRunRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.DeleteTransferRunRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.ListTransferConfigsRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false
  field :data_source_ids, 2, repeated: true, type: :string, json_name: "dataSourceIds"
  field :page_token, 3, type: :string, json_name: "pageToken"
  field :page_size, 4, type: :int32, json_name: "pageSize"
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.ListTransferConfigsResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :transfer_configs, 1,
    repeated: true,
    type: Google.Cloud.Bigquery.Datatransfer.V1.TransferConfig,
    json_name: "transferConfigs",
    deprecated: false

  field :next_page_token, 2, type: :string, json_name: "nextPageToken", deprecated: false
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.ListTransferRunsRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false

  field :states, 2,
    repeated: true,
    type: Google.Cloud.Bigquery.Datatransfer.V1.TransferState,
    enum: true

  field :page_token, 3, type: :string, json_name: "pageToken"
  field :page_size, 4, type: :int32, json_name: "pageSize"

  field :run_attempt, 5,
    type: Google.Cloud.Bigquery.Datatransfer.V1.ListTransferRunsRequest.RunAttempt,
    json_name: "runAttempt",
    enum: true
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.ListTransferRunsResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :transfer_runs, 1,
    repeated: true,
    type: Google.Cloud.Bigquery.Datatransfer.V1.TransferRun,
    json_name: "transferRuns",
    deprecated: false

  field :next_page_token, 2, type: :string, json_name: "nextPageToken", deprecated: false
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.ListTransferLogsRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false
  field :page_token, 4, type: :string, json_name: "pageToken"
  field :page_size, 5, type: :int32, json_name: "pageSize"

  field :message_types, 6,
    repeated: true,
    type: Google.Cloud.Bigquery.Datatransfer.V1.TransferMessage.MessageSeverity,
    json_name: "messageTypes",
    enum: true
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.ListTransferLogsResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :transfer_messages, 1,
    repeated: true,
    type: Google.Cloud.Bigquery.Datatransfer.V1.TransferMessage,
    json_name: "transferMessages",
    deprecated: false

  field :next_page_token, 2, type: :string, json_name: "nextPageToken", deprecated: false
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.CheckValidCredsRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.CheckValidCredsResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :has_valid_creds, 1, type: :bool, json_name: "hasValidCreds"
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.ScheduleTransferRunsRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false
  field :start_time, 2, type: Google.Protobuf.Timestamp, json_name: "startTime", deprecated: false
  field :end_time, 3, type: Google.Protobuf.Timestamp, json_name: "endTime", deprecated: false
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.ScheduleTransferRunsResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :runs, 1, repeated: true, type: Google.Cloud.Bigquery.Datatransfer.V1.TransferRun
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.StartManualTransferRunsRequest.TimeRange do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :start_time, 1, type: Google.Protobuf.Timestamp, json_name: "startTime"
  field :end_time, 2, type: Google.Protobuf.Timestamp, json_name: "endTime"
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.StartManualTransferRunsRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:time, 0)

  field :parent, 1, type: :string, deprecated: false

  field :requested_time_range, 3,
    type: Google.Cloud.Bigquery.Datatransfer.V1.StartManualTransferRunsRequest.TimeRange,
    json_name: "requestedTimeRange",
    oneof: 0

  field :requested_run_time, 4,
    type: Google.Protobuf.Timestamp,
    json_name: "requestedRunTime",
    oneof: 0
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.StartManualTransferRunsResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :runs, 1, repeated: true, type: Google.Cloud.Bigquery.Datatransfer.V1.TransferRun
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.EnrollDataSourcesRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
  field :data_source_ids, 2, repeated: true, type: :string, json_name: "dataSourceIds"
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.UnenrollDataSourcesRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
  field :data_source_ids, 2, repeated: true, type: :string, json_name: "dataSourceIds"
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.DataTransferService.Service do
  @moduledoc false

  use GRPC.Service,
    name: "google.cloud.bigquery.datatransfer.v1.DataTransferService",
    protoc_gen_elixir_version: "0.13.0"

  rpc(
    :GetDataSource,
    Google.Cloud.Bigquery.Datatransfer.V1.GetDataSourceRequest,
    Google.Cloud.Bigquery.Datatransfer.V1.DataSource
  )

  rpc(
    :ListDataSources,
    Google.Cloud.Bigquery.Datatransfer.V1.ListDataSourcesRequest,
    Google.Cloud.Bigquery.Datatransfer.V1.ListDataSourcesResponse
  )

  rpc(
    :CreateTransferConfig,
    Google.Cloud.Bigquery.Datatransfer.V1.CreateTransferConfigRequest,
    Google.Cloud.Bigquery.Datatransfer.V1.TransferConfig
  )

  rpc(
    :UpdateTransferConfig,
    Google.Cloud.Bigquery.Datatransfer.V1.UpdateTransferConfigRequest,
    Google.Cloud.Bigquery.Datatransfer.V1.TransferConfig
  )

  rpc(
    :DeleteTransferConfig,
    Google.Cloud.Bigquery.Datatransfer.V1.DeleteTransferConfigRequest,
    Google.Protobuf.Empty
  )

  rpc(
    :GetTransferConfig,
    Google.Cloud.Bigquery.Datatransfer.V1.GetTransferConfigRequest,
    Google.Cloud.Bigquery.Datatransfer.V1.TransferConfig
  )

  rpc(
    :ListTransferConfigs,
    Google.Cloud.Bigquery.Datatransfer.V1.ListTransferConfigsRequest,
    Google.Cloud.Bigquery.Datatransfer.V1.ListTransferConfigsResponse
  )

  rpc(
    :ScheduleTransferRuns,
    Google.Cloud.Bigquery.Datatransfer.V1.ScheduleTransferRunsRequest,
    Google.Cloud.Bigquery.Datatransfer.V1.ScheduleTransferRunsResponse
  )

  rpc(
    :StartManualTransferRuns,
    Google.Cloud.Bigquery.Datatransfer.V1.StartManualTransferRunsRequest,
    Google.Cloud.Bigquery.Datatransfer.V1.StartManualTransferRunsResponse
  )

  rpc(
    :GetTransferRun,
    Google.Cloud.Bigquery.Datatransfer.V1.GetTransferRunRequest,
    Google.Cloud.Bigquery.Datatransfer.V1.TransferRun
  )

  rpc(
    :DeleteTransferRun,
    Google.Cloud.Bigquery.Datatransfer.V1.DeleteTransferRunRequest,
    Google.Protobuf.Empty
  )

  rpc(
    :ListTransferRuns,
    Google.Cloud.Bigquery.Datatransfer.V1.ListTransferRunsRequest,
    Google.Cloud.Bigquery.Datatransfer.V1.ListTransferRunsResponse
  )

  rpc(
    :ListTransferLogs,
    Google.Cloud.Bigquery.Datatransfer.V1.ListTransferLogsRequest,
    Google.Cloud.Bigquery.Datatransfer.V1.ListTransferLogsResponse
  )

  rpc(
    :CheckValidCreds,
    Google.Cloud.Bigquery.Datatransfer.V1.CheckValidCredsRequest,
    Google.Cloud.Bigquery.Datatransfer.V1.CheckValidCredsResponse
  )

  rpc(
    :EnrollDataSources,
    Google.Cloud.Bigquery.Datatransfer.V1.EnrollDataSourcesRequest,
    Google.Protobuf.Empty
  )

  rpc(
    :UnenrollDataSources,
    Google.Cloud.Bigquery.Datatransfer.V1.UnenrollDataSourcesRequest,
    Google.Protobuf.Empty
  )
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.DataTransferService.Stub do
  @moduledoc false

  use GRPC.Stub, service: Google.Cloud.Bigquery.Datatransfer.V1.DataTransferService.Service
end
