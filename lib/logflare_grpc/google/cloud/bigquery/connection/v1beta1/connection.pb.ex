defmodule Google.Cloud.Bigquery.Connection.V1beta1.CloudSqlProperties.DatabaseType do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :DATABASE_TYPE_UNSPECIFIED, 0
  field :POSTGRES, 1
  field :MYSQL, 2
end

defmodule Google.Cloud.Bigquery.Connection.V1beta1.CreateConnectionRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false
  field :connection_id, 2, type: :string, json_name: "connectionId", deprecated: false

  field :connection, 3,
    type: Google.Cloud.Bigquery.Connection.V1beta1.Connection,
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Connection.V1beta1.GetConnectionRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Connection.V1beta1.ListConnectionsRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false

  field :max_results, 2,
    type: Google.Protobuf.UInt32Value,
    json_name: "maxResults",
    deprecated: false

  field :page_token, 3, type: :string, json_name: "pageToken"
end

defmodule Google.Cloud.Bigquery.Connection.V1beta1.ListConnectionsResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :next_page_token, 1, type: :string, json_name: "nextPageToken"
  field :connections, 2, repeated: true, type: Google.Cloud.Bigquery.Connection.V1beta1.Connection
end

defmodule Google.Cloud.Bigquery.Connection.V1beta1.UpdateConnectionRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false

  field :connection, 2,
    type: Google.Cloud.Bigquery.Connection.V1beta1.Connection,
    deprecated: false

  field :update_mask, 3,
    type: Google.Protobuf.FieldMask,
    json_name: "updateMask",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Connection.V1beta1.UpdateConnectionCredentialRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false

  field :credential, 2,
    type: Google.Cloud.Bigquery.Connection.V1beta1.ConnectionCredential,
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Connection.V1beta1.DeleteConnectionRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Connection.V1beta1.Connection do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:properties, 0)

  field :name, 1, type: :string
  field :friendly_name, 2, type: :string, json_name: "friendlyName"
  field :description, 3, type: :string

  field :cloud_sql, 4,
    type: Google.Cloud.Bigquery.Connection.V1beta1.CloudSqlProperties,
    json_name: "cloudSql",
    oneof: 0

  field :creation_time, 5, type: :int64, json_name: "creationTime", deprecated: false
  field :last_modified_time, 6, type: :int64, json_name: "lastModifiedTime", deprecated: false
  field :has_credential, 7, type: :bool, json_name: "hasCredential", deprecated: false
end

defmodule Google.Cloud.Bigquery.Connection.V1beta1.ConnectionCredential do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:credential, 0)

  field :cloud_sql, 1,
    type: Google.Cloud.Bigquery.Connection.V1beta1.CloudSqlCredential,
    json_name: "cloudSql",
    oneof: 0
end

defmodule Google.Cloud.Bigquery.Connection.V1beta1.CloudSqlProperties do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :instance_id, 1, type: :string, json_name: "instanceId"
  field :database, 2, type: :string

  field :type, 3,
    type: Google.Cloud.Bigquery.Connection.V1beta1.CloudSqlProperties.DatabaseType,
    enum: true

  field :credential, 4,
    type: Google.Cloud.Bigquery.Connection.V1beta1.CloudSqlCredential,
    deprecated: false

  field :service_account_id, 5, type: :string, json_name: "serviceAccountId", deprecated: false
end

defmodule Google.Cloud.Bigquery.Connection.V1beta1.CloudSqlCredential do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :username, 1, type: :string
  field :password, 2, type: :string
end

defmodule Google.Cloud.Bigquery.Connection.V1beta1.ConnectionService.Service do
  @moduledoc false

  use GRPC.Service,
    name: "google.cloud.bigquery.connection.v1beta1.ConnectionService",
    protoc_gen_elixir_version: "0.13.0"

  rpc(
    :CreateConnection,
    Google.Cloud.Bigquery.Connection.V1beta1.CreateConnectionRequest,
    Google.Cloud.Bigquery.Connection.V1beta1.Connection
  )

  rpc(
    :GetConnection,
    Google.Cloud.Bigquery.Connection.V1beta1.GetConnectionRequest,
    Google.Cloud.Bigquery.Connection.V1beta1.Connection
  )

  rpc(
    :ListConnections,
    Google.Cloud.Bigquery.Connection.V1beta1.ListConnectionsRequest,
    Google.Cloud.Bigquery.Connection.V1beta1.ListConnectionsResponse
  )

  rpc(
    :UpdateConnection,
    Google.Cloud.Bigquery.Connection.V1beta1.UpdateConnectionRequest,
    Google.Cloud.Bigquery.Connection.V1beta1.Connection
  )

  rpc(
    :UpdateConnectionCredential,
    Google.Cloud.Bigquery.Connection.V1beta1.UpdateConnectionCredentialRequest,
    Google.Protobuf.Empty
  )

  rpc(
    :DeleteConnection,
    Google.Cloud.Bigquery.Connection.V1beta1.DeleteConnectionRequest,
    Google.Protobuf.Empty
  )

  rpc(:GetIamPolicy, Google.Iam.V1.GetIamPolicyRequest, Google.Iam.V1.Policy)

  rpc(:SetIamPolicy, Google.Iam.V1.SetIamPolicyRequest, Google.Iam.V1.Policy)

  rpc(
    :TestIamPermissions,
    Google.Iam.V1.TestIamPermissionsRequest,
    Google.Iam.V1.TestIamPermissionsResponse
  )
end

defmodule Google.Cloud.Bigquery.Connection.V1beta1.ConnectionService.Stub do
  @moduledoc false

  use GRPC.Stub, service: Google.Cloud.Bigquery.Connection.V1beta1.ConnectionService.Service
end
