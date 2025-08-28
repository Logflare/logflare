defmodule Google.Cloud.Bigquery.V2.DatasetAccessEntry.TargetType do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :TARGET_TYPE_UNSPECIFIED, 0
  field :VIEWS, 1
  field :ROUTINES, 2
end

defmodule Google.Cloud.Bigquery.V2.Dataset.StorageBillingModel do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :STORAGE_BILLING_MODEL_UNSPECIFIED, 0
  field :LOGICAL, 1
  field :PHYSICAL, 2
end

defmodule Google.Cloud.Bigquery.V2.LinkedDatasetMetadata.LinkState do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :LINK_STATE_UNSPECIFIED, 0
  field :LINKED, 1
  field :UNLINKED, 2
end

defmodule Google.Cloud.Bigquery.V2.GetDatasetRequest.DatasetView do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :DATASET_VIEW_UNSPECIFIED, 0
  field :METADATA, 1
  field :ACL, 2
  field :FULL, 3
end

defmodule Google.Cloud.Bigquery.V2.UpdateOrPatchDatasetRequest.UpdateMode do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :UPDATE_MODE_UNSPECIFIED, 0
  field :UPDATE_METADATA, 1
  field :UPDATE_ACL, 2
  field :UPDATE_FULL, 3
end

defmodule Google.Cloud.Bigquery.V2.DatasetAccessEntry do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :dataset, 1, type: Google.Cloud.Bigquery.V2.DatasetReference

  field :target_types, 2,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.DatasetAccessEntry.TargetType,
    json_name: "targetTypes",
    enum: true
end

defmodule Google.Cloud.Bigquery.V2.Access do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :role, 1, type: :string
  field :user_by_email, 2, type: :string, json_name: "userByEmail"
  field :group_by_email, 3, type: :string, json_name: "groupByEmail"
  field :domain, 4, type: :string
  field :special_group, 5, type: :string, json_name: "specialGroup"
  field :iam_member, 7, type: :string, json_name: "iamMember"
  field :view, 6, type: Google.Cloud.Bigquery.V2.TableReference
  field :routine, 8, type: Google.Cloud.Bigquery.V2.RoutineReference
  field :dataset, 9, type: Google.Cloud.Bigquery.V2.DatasetAccessEntry
  field :condition, 10, type: Google.Type.Expr, deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.Dataset.LabelsEntry do
  @moduledoc false

  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Google.Cloud.Bigquery.V2.Dataset.ResourceTagsEntry do
  @moduledoc false

  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Google.Cloud.Bigquery.V2.Dataset do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :kind, 1, type: :string, deprecated: false
  field :etag, 2, type: :string, deprecated: false
  field :id, 3, type: :string, deprecated: false
  field :self_link, 4, type: :string, json_name: "selfLink", deprecated: false

  field :dataset_reference, 5,
    type: Google.Cloud.Bigquery.V2.DatasetReference,
    json_name: "datasetReference",
    deprecated: false

  field :friendly_name, 6,
    type: Google.Protobuf.StringValue,
    json_name: "friendlyName",
    deprecated: false

  field :description, 7, type: Google.Protobuf.StringValue, deprecated: false

  field :default_table_expiration_ms, 8,
    type: Google.Protobuf.Int64Value,
    json_name: "defaultTableExpirationMs",
    deprecated: false

  field :default_partition_expiration_ms, 14,
    type: Google.Protobuf.Int64Value,
    json_name: "defaultPartitionExpirationMs"

  field :labels, 9, repeated: true, type: Google.Cloud.Bigquery.V2.Dataset.LabelsEntry, map: true
  field :access, 10, repeated: true, type: Google.Cloud.Bigquery.V2.Access, deprecated: false
  field :creation_time, 11, type: :int64, json_name: "creationTime", deprecated: false
  field :last_modified_time, 12, type: :int64, json_name: "lastModifiedTime", deprecated: false
  field :location, 13, type: :string

  field :default_encryption_configuration, 16,
    type: Google.Cloud.Bigquery.V2.EncryptionConfiguration,
    json_name: "defaultEncryptionConfiguration"

  field :satisfies_pzs, 17,
    type: Google.Protobuf.BoolValue,
    json_name: "satisfiesPzs",
    deprecated: false

  field :satisfies_pzi, 31,
    type: Google.Protobuf.BoolValue,
    json_name: "satisfiesPzi",
    deprecated: false

  field :type, 18, type: :string, deprecated: false

  field :linked_dataset_source, 19,
    type: Google.Cloud.Bigquery.V2.LinkedDatasetSource,
    json_name: "linkedDatasetSource",
    deprecated: false

  field :linked_dataset_metadata, 29,
    type: Google.Cloud.Bigquery.V2.LinkedDatasetMetadata,
    json_name: "linkedDatasetMetadata",
    deprecated: false

  field :external_dataset_reference, 20,
    type: Google.Cloud.Bigquery.V2.ExternalDatasetReference,
    json_name: "externalDatasetReference",
    deprecated: false

  field :external_catalog_dataset_options, 32,
    type: Google.Cloud.Bigquery.V2.ExternalCatalogDatasetOptions,
    json_name: "externalCatalogDatasetOptions",
    deprecated: false

  field :is_case_insensitive, 21,
    type: Google.Protobuf.BoolValue,
    json_name: "isCaseInsensitive",
    deprecated: false

  field :default_collation, 22,
    type: Google.Protobuf.StringValue,
    json_name: "defaultCollation",
    deprecated: false

  field :default_rounding_mode, 26,
    type: Google.Cloud.Bigquery.V2.TableFieldSchema.RoundingMode,
    json_name: "defaultRoundingMode",
    enum: true,
    deprecated: false

  field :max_time_travel_hours, 23,
    type: Google.Protobuf.Int64Value,
    json_name: "maxTimeTravelHours",
    deprecated: false

  field :tags, 24, repeated: true, type: Google.Cloud.Bigquery.V2.GcpTag, deprecated: true

  field :storage_billing_model, 25,
    type: Google.Cloud.Bigquery.V2.Dataset.StorageBillingModel,
    json_name: "storageBillingModel",
    enum: true,
    deprecated: false

  field :restrictions, 27, type: Google.Cloud.Bigquery.V2.RestrictionConfig, deprecated: false

  field :resource_tags, 30,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.Dataset.ResourceTagsEntry,
    json_name: "resourceTags",
    map: true,
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.GcpTag do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :tag_key, 1, type: :string, json_name: "tagKey", deprecated: false
  field :tag_value, 2, type: :string, json_name: "tagValue", deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.LinkedDatasetSource do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :source_dataset, 1,
    type: Google.Cloud.Bigquery.V2.DatasetReference,
    json_name: "sourceDataset"
end

defmodule Google.Cloud.Bigquery.V2.LinkedDatasetMetadata do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :link_state, 1,
    type: Google.Cloud.Bigquery.V2.LinkedDatasetMetadata.LinkState,
    json_name: "linkState",
    enum: true,
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.GetDatasetRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId", deprecated: false
  field :dataset_id, 2, type: :string, json_name: "datasetId", deprecated: false

  field :dataset_view, 3,
    type: Google.Cloud.Bigquery.V2.GetDatasetRequest.DatasetView,
    json_name: "datasetView",
    enum: true,
    deprecated: false

  field :access_policy_version, 4,
    type: :int32,
    json_name: "accessPolicyVersion",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.InsertDatasetRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId", deprecated: false
  field :dataset, 2, type: Google.Cloud.Bigquery.V2.Dataset, deprecated: false

  field :access_policy_version, 4,
    type: :int32,
    json_name: "accessPolicyVersion",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.UpdateOrPatchDatasetRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId", deprecated: false
  field :dataset_id, 2, type: :string, json_name: "datasetId", deprecated: false
  field :dataset, 3, type: Google.Cloud.Bigquery.V2.Dataset, deprecated: false

  field :update_mode, 4,
    type: Google.Cloud.Bigquery.V2.UpdateOrPatchDatasetRequest.UpdateMode,
    json_name: "updateMode",
    enum: true,
    deprecated: false

  field :access_policy_version, 5,
    type: :int32,
    json_name: "accessPolicyVersion",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.DeleteDatasetRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId", deprecated: false
  field :dataset_id, 2, type: :string, json_name: "datasetId", deprecated: false
  field :delete_contents, 3, type: :bool, json_name: "deleteContents"
end

defmodule Google.Cloud.Bigquery.V2.ListDatasetsRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId", deprecated: false
  field :max_results, 2, type: Google.Protobuf.UInt32Value, json_name: "maxResults"
  field :page_token, 3, type: :string, json_name: "pageToken"
  field :all, 4, type: :bool
  field :filter, 5, type: :string
end

defmodule Google.Cloud.Bigquery.V2.ListFormatDataset.LabelsEntry do
  @moduledoc false

  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Google.Cloud.Bigquery.V2.ListFormatDataset do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :kind, 1, type: :string
  field :id, 2, type: :string

  field :dataset_reference, 3,
    type: Google.Cloud.Bigquery.V2.DatasetReference,
    json_name: "datasetReference"

  field :labels, 4,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.ListFormatDataset.LabelsEntry,
    map: true

  field :friendly_name, 5, type: Google.Protobuf.StringValue, json_name: "friendlyName"
  field :location, 6, type: :string

  field :external_dataset_reference, 11,
    type: Google.Cloud.Bigquery.V2.ExternalDatasetReference,
    json_name: "externalDatasetReference",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.DatasetList do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :kind, 1, type: :string, deprecated: false
  field :etag, 2, type: :string, deprecated: false
  field :next_page_token, 3, type: :string, json_name: "nextPageToken"
  field :datasets, 4, repeated: true, type: Google.Cloud.Bigquery.V2.ListFormatDataset
  field :unreachable, 5, repeated: true, type: :string
end

defmodule Google.Cloud.Bigquery.V2.UndeleteDatasetRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId", deprecated: false
  field :dataset_id, 2, type: :string, json_name: "datasetId", deprecated: false

  field :deletion_time, 3,
    type: Google.Protobuf.Timestamp,
    json_name: "deletionTime",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.DatasetService.Service do
  @moduledoc false

  use GRPC.Service,
    name: "google.cloud.bigquery.v2.DatasetService",
    protoc_gen_elixir_version: "0.13.0"

  rpc(:GetDataset, Google.Cloud.Bigquery.V2.GetDatasetRequest, Google.Cloud.Bigquery.V2.Dataset)

  rpc(
    :InsertDataset,
    Google.Cloud.Bigquery.V2.InsertDatasetRequest,
    Google.Cloud.Bigquery.V2.Dataset
  )

  rpc(
    :PatchDataset,
    Google.Cloud.Bigquery.V2.UpdateOrPatchDatasetRequest,
    Google.Cloud.Bigquery.V2.Dataset
  )

  rpc(
    :UpdateDataset,
    Google.Cloud.Bigquery.V2.UpdateOrPatchDatasetRequest,
    Google.Cloud.Bigquery.V2.Dataset
  )

  rpc(:DeleteDataset, Google.Cloud.Bigquery.V2.DeleteDatasetRequest, Google.Protobuf.Empty)

  rpc(
    :ListDatasets,
    Google.Cloud.Bigquery.V2.ListDatasetsRequest,
    Google.Cloud.Bigquery.V2.DatasetList
  )

  rpc(
    :UndeleteDataset,
    Google.Cloud.Bigquery.V2.UndeleteDatasetRequest,
    Google.Cloud.Bigquery.V2.Dataset
  )
end

defmodule Google.Cloud.Bigquery.V2.DatasetService.Stub do
  @moduledoc false

  use GRPC.Stub, service: Google.Cloud.Bigquery.V2.DatasetService.Service
end
