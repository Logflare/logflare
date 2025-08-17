defmodule Google.Cloud.Bigquery.Datapolicies.V1beta1.DataPolicy.DataPolicyType do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :DATA_POLICY_TYPE_UNSPECIFIED, 0
  field :COLUMN_LEVEL_SECURITY_POLICY, 3
  field :DATA_MASKING_POLICY, 2
end

defmodule Google.Cloud.Bigquery.Datapolicies.V1beta1.DataMaskingPolicy.PredefinedExpression do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :PREDEFINED_EXPRESSION_UNSPECIFIED, 0
  field :SHA256, 3
  field :ALWAYS_NULL, 5
  field :DEFAULT_MASKING_VALUE, 7
end

defmodule Google.Cloud.Bigquery.Datapolicies.V1beta1.CreateDataPolicyRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false

  field :data_policy, 2,
    type: Google.Cloud.Bigquery.Datapolicies.V1beta1.DataPolicy,
    json_name: "dataPolicy",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Datapolicies.V1beta1.UpdateDataPolicyRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :data_policy, 1,
    type: Google.Cloud.Bigquery.Datapolicies.V1beta1.DataPolicy,
    json_name: "dataPolicy",
    deprecated: false

  field :update_mask, 2, type: Google.Protobuf.FieldMask, json_name: "updateMask"
end

defmodule Google.Cloud.Bigquery.Datapolicies.V1beta1.DeleteDataPolicyRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Datapolicies.V1beta1.GetDataPolicyRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Datapolicies.V1beta1.ListDataPoliciesRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false
  field :page_size, 2, type: :int32, json_name: "pageSize"
  field :page_token, 3, type: :string, json_name: "pageToken"
end

defmodule Google.Cloud.Bigquery.Datapolicies.V1beta1.ListDataPoliciesResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :data_policies, 1,
    repeated: true,
    type: Google.Cloud.Bigquery.Datapolicies.V1beta1.DataPolicy,
    json_name: "dataPolicies"

  field :next_page_token, 2, type: :string, json_name: "nextPageToken"
end

defmodule Google.Cloud.Bigquery.Datapolicies.V1beta1.DataPolicy do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:matching_label, 0)

  oneof(:policy, 1)

  field :policy_tag, 4, type: :string, json_name: "policyTag", oneof: 0

  field :data_masking_policy, 5,
    type: Google.Cloud.Bigquery.Datapolicies.V1beta1.DataMaskingPolicy,
    json_name: "dataMaskingPolicy",
    oneof: 1

  field :name, 1, type: :string, deprecated: false

  field :data_policy_type, 2,
    type: Google.Cloud.Bigquery.Datapolicies.V1beta1.DataPolicy.DataPolicyType,
    json_name: "dataPolicyType",
    enum: true

  field :data_policy_id, 3, type: :string, json_name: "dataPolicyId"
end

defmodule Google.Cloud.Bigquery.Datapolicies.V1beta1.DataMaskingPolicy do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:masking_expression, 0)

  field :predefined_expression, 1,
    type: Google.Cloud.Bigquery.Datapolicies.V1beta1.DataMaskingPolicy.PredefinedExpression,
    json_name: "predefinedExpression",
    enum: true,
    oneof: 0
end

defmodule Google.Cloud.Bigquery.Datapolicies.V1beta1.DataPolicyService.Service do
  @moduledoc false

  use GRPC.Service,
    name: "google.cloud.bigquery.datapolicies.v1beta1.DataPolicyService",
    protoc_gen_elixir_version: "0.13.0"

  rpc(
    :CreateDataPolicy,
    Google.Cloud.Bigquery.Datapolicies.V1beta1.CreateDataPolicyRequest,
    Google.Cloud.Bigquery.Datapolicies.V1beta1.DataPolicy
  )

  rpc(
    :UpdateDataPolicy,
    Google.Cloud.Bigquery.Datapolicies.V1beta1.UpdateDataPolicyRequest,
    Google.Cloud.Bigquery.Datapolicies.V1beta1.DataPolicy
  )

  rpc(
    :DeleteDataPolicy,
    Google.Cloud.Bigquery.Datapolicies.V1beta1.DeleteDataPolicyRequest,
    Google.Protobuf.Empty
  )

  rpc(
    :GetDataPolicy,
    Google.Cloud.Bigquery.Datapolicies.V1beta1.GetDataPolicyRequest,
    Google.Cloud.Bigquery.Datapolicies.V1beta1.DataPolicy
  )

  rpc(
    :ListDataPolicies,
    Google.Cloud.Bigquery.Datapolicies.V1beta1.ListDataPoliciesRequest,
    Google.Cloud.Bigquery.Datapolicies.V1beta1.ListDataPoliciesResponse
  )

  rpc(:GetIamPolicy, Google.Iam.V1.GetIamPolicyRequest, Google.Iam.V1.Policy)

  rpc(:SetIamPolicy, Google.Iam.V1.SetIamPolicyRequest, Google.Iam.V1.Policy)

  rpc(
    :TestIamPermissions,
    Google.Iam.V1.TestIamPermissionsRequest,
    Google.Iam.V1.TestIamPermissionsResponse
  )
end

defmodule Google.Cloud.Bigquery.Datapolicies.V1beta1.DataPolicyService.Stub do
  @moduledoc false

  use GRPC.Stub, service: Google.Cloud.Bigquery.Datapolicies.V1beta1.DataPolicyService.Service
end
