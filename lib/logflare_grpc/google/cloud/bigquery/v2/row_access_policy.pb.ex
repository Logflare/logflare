defmodule Google.Cloud.Bigquery.V2.ListRowAccessPoliciesRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId", deprecated: false
  field :dataset_id, 2, type: :string, json_name: "datasetId", deprecated: false
  field :table_id, 3, type: :string, json_name: "tableId", deprecated: false
  field :page_token, 4, type: :string, json_name: "pageToken"
  field :page_size, 5, type: :int32, json_name: "pageSize"
end

defmodule Google.Cloud.Bigquery.V2.ListRowAccessPoliciesResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :row_access_policies, 1,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.RowAccessPolicy,
    json_name: "rowAccessPolicies"

  field :next_page_token, 2, type: :string, json_name: "nextPageToken"
end

defmodule Google.Cloud.Bigquery.V2.GetRowAccessPolicyRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId", deprecated: false
  field :dataset_id, 2, type: :string, json_name: "datasetId", deprecated: false
  field :table_id, 3, type: :string, json_name: "tableId", deprecated: false
  field :policy_id, 4, type: :string, json_name: "policyId", deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.CreateRowAccessPolicyRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId", deprecated: false
  field :dataset_id, 2, type: :string, json_name: "datasetId", deprecated: false
  field :table_id, 3, type: :string, json_name: "tableId", deprecated: false

  field :row_access_policy, 4,
    type: Google.Cloud.Bigquery.V2.RowAccessPolicy,
    json_name: "rowAccessPolicy",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.UpdateRowAccessPolicyRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId", deprecated: false
  field :dataset_id, 2, type: :string, json_name: "datasetId", deprecated: false
  field :table_id, 3, type: :string, json_name: "tableId", deprecated: false
  field :policy_id, 4, type: :string, json_name: "policyId", deprecated: false

  field :row_access_policy, 5,
    type: Google.Cloud.Bigquery.V2.RowAccessPolicy,
    json_name: "rowAccessPolicy",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.DeleteRowAccessPolicyRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId", deprecated: false
  field :dataset_id, 2, type: :string, json_name: "datasetId", deprecated: false
  field :table_id, 3, type: :string, json_name: "tableId", deprecated: false
  field :policy_id, 4, type: :string, json_name: "policyId", deprecated: false
  field :force, 5, proto3_optional: true, type: :bool
end

defmodule Google.Cloud.Bigquery.V2.BatchDeleteRowAccessPoliciesRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId", deprecated: false
  field :dataset_id, 2, type: :string, json_name: "datasetId", deprecated: false
  field :table_id, 3, type: :string, json_name: "tableId", deprecated: false
  field :policy_ids, 4, repeated: true, type: :string, json_name: "policyIds", deprecated: false
  field :force, 5, proto3_optional: true, type: :bool
end

defmodule Google.Cloud.Bigquery.V2.RowAccessPolicy do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :etag, 1, type: :string, deprecated: false

  field :row_access_policy_reference, 2,
    type: Google.Cloud.Bigquery.V2.RowAccessPolicyReference,
    json_name: "rowAccessPolicyReference",
    deprecated: false

  field :filter_predicate, 3, type: :string, json_name: "filterPredicate", deprecated: false

  field :creation_time, 4,
    type: Google.Protobuf.Timestamp,
    json_name: "creationTime",
    deprecated: false

  field :last_modified_time, 5,
    type: Google.Protobuf.Timestamp,
    json_name: "lastModifiedTime",
    deprecated: false

  field :grantees, 6, repeated: true, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.RowAccessPolicyService.Service do
  @moduledoc false

  use GRPC.Service,
    name: "google.cloud.bigquery.v2.RowAccessPolicyService",
    protoc_gen_elixir_version: "0.13.0"

  rpc(
    :ListRowAccessPolicies,
    Google.Cloud.Bigquery.V2.ListRowAccessPoliciesRequest,
    Google.Cloud.Bigquery.V2.ListRowAccessPoliciesResponse
  )

  rpc(
    :GetRowAccessPolicy,
    Google.Cloud.Bigquery.V2.GetRowAccessPolicyRequest,
    Google.Cloud.Bigquery.V2.RowAccessPolicy
  )

  rpc(
    :CreateRowAccessPolicy,
    Google.Cloud.Bigquery.V2.CreateRowAccessPolicyRequest,
    Google.Cloud.Bigquery.V2.RowAccessPolicy
  )

  rpc(
    :UpdateRowAccessPolicy,
    Google.Cloud.Bigquery.V2.UpdateRowAccessPolicyRequest,
    Google.Cloud.Bigquery.V2.RowAccessPolicy
  )

  rpc(
    :DeleteRowAccessPolicy,
    Google.Cloud.Bigquery.V2.DeleteRowAccessPolicyRequest,
    Google.Protobuf.Empty
  )

  rpc(
    :BatchDeleteRowAccessPolicies,
    Google.Cloud.Bigquery.V2.BatchDeleteRowAccessPoliciesRequest,
    Google.Protobuf.Empty
  )
end

defmodule Google.Cloud.Bigquery.V2.RowAccessPolicyService.Stub do
  @moduledoc false

  use GRPC.Stub, service: Google.Cloud.Bigquery.V2.RowAccessPolicyService.Service
end
