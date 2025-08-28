defmodule Google.Cloud.Bigquery.Datatransfer.V1.TransferType do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :TRANSFER_TYPE_UNSPECIFIED, 0
  field :BATCH, 1
  field :STREAMING, 2
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.TransferState do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :TRANSFER_STATE_UNSPECIFIED, 0
  field :PENDING, 2
  field :RUNNING, 3
  field :SUCCEEDED, 4
  field :FAILED, 5
  field :CANCELLED, 6
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.TransferMessage.MessageSeverity do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :MESSAGE_SEVERITY_UNSPECIFIED, 0
  field :INFO, 1
  field :WARNING, 2
  field :ERROR, 3
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.EmailPreferences do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :enable_failure_email, 1, type: :bool, json_name: "enableFailureEmail"
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.ScheduleOptions do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :disable_auto_scheduling, 3, type: :bool, json_name: "disableAutoScheduling"
  field :start_time, 1, type: Google.Protobuf.Timestamp, json_name: "startTime"
  field :end_time, 2, type: Google.Protobuf.Timestamp, json_name: "endTime"
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.ScheduleOptionsV2 do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:schedule, 0)

  field :time_based_schedule, 1,
    type: Google.Cloud.Bigquery.Datatransfer.V1.TimeBasedSchedule,
    json_name: "timeBasedSchedule",
    oneof: 0

  field :manual_schedule, 2,
    type: Google.Cloud.Bigquery.Datatransfer.V1.ManualSchedule,
    json_name: "manualSchedule",
    oneof: 0

  field :event_driven_schedule, 3,
    type: Google.Cloud.Bigquery.Datatransfer.V1.EventDrivenSchedule,
    json_name: "eventDrivenSchedule",
    oneof: 0
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.TimeBasedSchedule do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :schedule, 1, type: :string
  field :start_time, 2, type: Google.Protobuf.Timestamp, json_name: "startTime"
  field :end_time, 3, type: Google.Protobuf.Timestamp, json_name: "endTime"
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.ManualSchedule do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.EventDrivenSchedule do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :pubsub_subscription, 1, type: :string, json_name: "pubsubSubscription"
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.UserInfo do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :email, 1, proto3_optional: true, type: :string
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.TransferConfig do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:destination, 0)

  field :name, 1, type: :string, deprecated: false
  field :destination_dataset_id, 2, type: :string, json_name: "destinationDatasetId", oneof: 0
  field :display_name, 3, type: :string, json_name: "displayName"
  field :data_source_id, 5, type: :string, json_name: "dataSourceId"
  field :params, 9, type: Google.Protobuf.Struct
  field :schedule, 7, type: :string

  field :schedule_options, 24,
    type: Google.Cloud.Bigquery.Datatransfer.V1.ScheduleOptions,
    json_name: "scheduleOptions"

  field :schedule_options_v2, 31,
    type: Google.Cloud.Bigquery.Datatransfer.V1.ScheduleOptionsV2,
    json_name: "scheduleOptionsV2"

  field :data_refresh_window_days, 12, type: :int32, json_name: "dataRefreshWindowDays"
  field :disabled, 13, type: :bool

  field :update_time, 4,
    type: Google.Protobuf.Timestamp,
    json_name: "updateTime",
    deprecated: false

  field :next_run_time, 8,
    type: Google.Protobuf.Timestamp,
    json_name: "nextRunTime",
    deprecated: false

  field :state, 10,
    type: Google.Cloud.Bigquery.Datatransfer.V1.TransferState,
    enum: true,
    deprecated: false

  field :user_id, 11, type: :int64, json_name: "userId"
  field :dataset_region, 14, type: :string, json_name: "datasetRegion", deprecated: false
  field :notification_pubsub_topic, 15, type: :string, json_name: "notificationPubsubTopic"

  field :email_preferences, 18,
    type: Google.Cloud.Bigquery.Datatransfer.V1.EmailPreferences,
    json_name: "emailPreferences"

  field :owner_info, 27,
    proto3_optional: true,
    type: Google.Cloud.Bigquery.Datatransfer.V1.UserInfo,
    json_name: "ownerInfo",
    deprecated: false

  field :encryption_configuration, 28,
    type: Google.Cloud.Bigquery.Datatransfer.V1.EncryptionConfiguration,
    json_name: "encryptionConfiguration"

  field :error, 32, type: Google.Rpc.Status, deprecated: false
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.EncryptionConfiguration do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :kms_key_name, 1, type: Google.Protobuf.StringValue, json_name: "kmsKeyName"
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.TransferRun do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:destination, 0)

  field :name, 1, type: :string, deprecated: false
  field :schedule_time, 3, type: Google.Protobuf.Timestamp, json_name: "scheduleTime"
  field :run_time, 10, type: Google.Protobuf.Timestamp, json_name: "runTime"
  field :error_status, 21, type: Google.Rpc.Status, json_name: "errorStatus"
  field :start_time, 4, type: Google.Protobuf.Timestamp, json_name: "startTime", deprecated: false
  field :end_time, 5, type: Google.Protobuf.Timestamp, json_name: "endTime", deprecated: false

  field :update_time, 6,
    type: Google.Protobuf.Timestamp,
    json_name: "updateTime",
    deprecated: false

  field :params, 9, type: Google.Protobuf.Struct, deprecated: false

  field :destination_dataset_id, 2,
    type: :string,
    json_name: "destinationDatasetId",
    oneof: 0,
    deprecated: false

  field :data_source_id, 7, type: :string, json_name: "dataSourceId", deprecated: false
  field :state, 8, type: Google.Cloud.Bigquery.Datatransfer.V1.TransferState, enum: true
  field :user_id, 11, type: :int64, json_name: "userId"
  field :schedule, 12, type: :string, deprecated: false

  field :notification_pubsub_topic, 23,
    type: :string,
    json_name: "notificationPubsubTopic",
    deprecated: false

  field :email_preferences, 25,
    type: Google.Cloud.Bigquery.Datatransfer.V1.EmailPreferences,
    json_name: "emailPreferences",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Datatransfer.V1.TransferMessage do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :message_time, 1, type: Google.Protobuf.Timestamp, json_name: "messageTime"

  field :severity, 2,
    type: Google.Cloud.Bigquery.Datatransfer.V1.TransferMessage.MessageSeverity,
    enum: true

  field :message_text, 3, type: :string, json_name: "messageText"
end
