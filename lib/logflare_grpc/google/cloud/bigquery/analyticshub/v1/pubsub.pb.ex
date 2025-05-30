defmodule Google.Cloud.Bigquery.Analyticshub.V1.PubSubSubscription.LabelsEntry do
  @moduledoc false

  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.PubSubSubscription do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false

  field :push_config, 4,
    type: Google.Cloud.Bigquery.Analyticshub.V1.PushConfig,
    json_name: "pushConfig",
    deprecated: false

  field :bigquery_config, 18,
    type: Google.Cloud.Bigquery.Analyticshub.V1.BigQueryConfig,
    json_name: "bigqueryConfig",
    deprecated: false

  field :cloud_storage_config, 22,
    type: Google.Cloud.Bigquery.Analyticshub.V1.CloudStorageConfig,
    json_name: "cloudStorageConfig",
    deprecated: false

  field :ack_deadline_seconds, 5, type: :int32, json_name: "ackDeadlineSeconds", deprecated: false

  field :retain_acked_messages, 7,
    type: :bool,
    json_name: "retainAckedMessages",
    deprecated: false

  field :message_retention_duration, 8,
    type: Google.Protobuf.Duration,
    json_name: "messageRetentionDuration",
    deprecated: false

  field :labels, 9,
    repeated: true,
    type: Google.Cloud.Bigquery.Analyticshub.V1.PubSubSubscription.LabelsEntry,
    map: true,
    deprecated: false

  field :enable_message_ordering, 10,
    type: :bool,
    json_name: "enableMessageOrdering",
    deprecated: false

  field :expiration_policy, 11,
    type: Google.Cloud.Bigquery.Analyticshub.V1.ExpirationPolicy,
    json_name: "expirationPolicy",
    deprecated: false

  field :filter, 12, type: :string, deprecated: false

  field :dead_letter_policy, 13,
    type: Google.Cloud.Bigquery.Analyticshub.V1.DeadLetterPolicy,
    json_name: "deadLetterPolicy",
    deprecated: false

  field :retry_policy, 14,
    type: Google.Cloud.Bigquery.Analyticshub.V1.RetryPolicy,
    json_name: "retryPolicy",
    deprecated: false

  field :detached, 15, type: :bool, deprecated: false

  field :enable_exactly_once_delivery, 16,
    type: :bool,
    json_name: "enableExactlyOnceDelivery",
    deprecated: false

  field :message_transforms, 25,
    repeated: true,
    type: Google.Cloud.Bigquery.Analyticshub.V1.MessageTransform,
    json_name: "messageTransforms",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.RetryPolicy do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :minimum_backoff, 1,
    type: Google.Protobuf.Duration,
    json_name: "minimumBackoff",
    deprecated: false

  field :maximum_backoff, 2,
    type: Google.Protobuf.Duration,
    json_name: "maximumBackoff",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.DeadLetterPolicy do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :dead_letter_topic, 1, type: :string, json_name: "deadLetterTopic", deprecated: false

  field :max_delivery_attempts, 2,
    type: :int32,
    json_name: "maxDeliveryAttempts",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.ExpirationPolicy do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :ttl, 1, type: Google.Protobuf.Duration, deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.PushConfig.OidcToken do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :service_account_email, 1,
    type: :string,
    json_name: "serviceAccountEmail",
    deprecated: false

  field :audience, 2, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.PushConfig.PubsubWrapper do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.PushConfig.NoWrapper do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :write_metadata, 1, type: :bool, json_name: "writeMetadata", deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.PushConfig.AttributesEntry do
  @moduledoc false

  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.PushConfig do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:authentication_method, 0)

  oneof(:wrapper, 1)

  field :oidc_token, 3,
    type: Google.Cloud.Bigquery.Analyticshub.V1.PushConfig.OidcToken,
    json_name: "oidcToken",
    oneof: 0,
    deprecated: false

  field :pubsub_wrapper, 4,
    type: Google.Cloud.Bigquery.Analyticshub.V1.PushConfig.PubsubWrapper,
    json_name: "pubsubWrapper",
    oneof: 1,
    deprecated: false

  field :no_wrapper, 5,
    type: Google.Cloud.Bigquery.Analyticshub.V1.PushConfig.NoWrapper,
    json_name: "noWrapper",
    oneof: 1,
    deprecated: false

  field :push_endpoint, 1, type: :string, json_name: "pushEndpoint", deprecated: false

  field :attributes, 2,
    repeated: true,
    type: Google.Cloud.Bigquery.Analyticshub.V1.PushConfig.AttributesEntry,
    map: true,
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.BigQueryConfig do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :table, 1, type: :string, deprecated: false
  field :use_topic_schema, 2, type: :bool, json_name: "useTopicSchema", deprecated: false
  field :write_metadata, 3, type: :bool, json_name: "writeMetadata", deprecated: false
  field :drop_unknown_fields, 4, type: :bool, json_name: "dropUnknownFields", deprecated: false
  field :use_table_schema, 6, type: :bool, json_name: "useTableSchema", deprecated: false

  field :service_account_email, 7,
    type: :string,
    json_name: "serviceAccountEmail",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.CloudStorageConfig.TextConfig do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.CloudStorageConfig.AvroConfig do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :write_metadata, 1, type: :bool, json_name: "writeMetadata", deprecated: false
  field :use_topic_schema, 2, type: :bool, json_name: "useTopicSchema", deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.CloudStorageConfig do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:output_format, 0)

  field :text_config, 4,
    type: Google.Cloud.Bigquery.Analyticshub.V1.CloudStorageConfig.TextConfig,
    json_name: "textConfig",
    oneof: 0,
    deprecated: false

  field :avro_config, 5,
    type: Google.Cloud.Bigquery.Analyticshub.V1.CloudStorageConfig.AvroConfig,
    json_name: "avroConfig",
    oneof: 0,
    deprecated: false

  field :bucket, 1, type: :string, deprecated: false
  field :filename_prefix, 2, type: :string, json_name: "filenamePrefix", deprecated: false
  field :filename_suffix, 3, type: :string, json_name: "filenameSuffix", deprecated: false

  field :filename_datetime_format, 10,
    type: :string,
    json_name: "filenameDatetimeFormat",
    deprecated: false

  field :max_duration, 6,
    type: Google.Protobuf.Duration,
    json_name: "maxDuration",
    deprecated: false

  field :max_bytes, 7, type: :int64, json_name: "maxBytes", deprecated: false
  field :max_messages, 8, type: :int64, json_name: "maxMessages", deprecated: false

  field :service_account_email, 11,
    type: :string,
    json_name: "serviceAccountEmail",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.MessageTransform do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:transform, 0)

  field :javascript_udf, 2,
    type: Google.Cloud.Bigquery.Analyticshub.V1.JavaScriptUDF,
    json_name: "javascriptUdf",
    oneof: 0,
    deprecated: false

  field :enabled, 3, type: :bool, deprecated: true
  field :disabled, 4, type: :bool, deprecated: false
end

defmodule Google.Cloud.Bigquery.Analyticshub.V1.JavaScriptUDF do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :function_name, 1, type: :string, json_name: "functionName", deprecated: false
  field :code, 2, type: :string, deprecated: false
end
