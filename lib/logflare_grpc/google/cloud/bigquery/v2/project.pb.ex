defmodule Google.Cloud.Bigquery.V2.GetServiceAccountRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId", deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.GetServiceAccountResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :kind, 1, type: :string
  field :email, 2, type: :string
end

defmodule Google.Cloud.Bigquery.V2.ProjectService.Service do
  @moduledoc false

  use GRPC.Service,
    name: "google.cloud.bigquery.v2.ProjectService",
    protoc_gen_elixir_version: "0.13.0"

  rpc(
    :GetServiceAccount,
    Google.Cloud.Bigquery.V2.GetServiceAccountRequest,
    Google.Cloud.Bigquery.V2.GetServiceAccountResponse
  )
end

defmodule Google.Cloud.Bigquery.V2.ProjectService.Stub do
  @moduledoc false

  use GRPC.Stub, service: Google.Cloud.Bigquery.V2.ProjectService.Service
end
