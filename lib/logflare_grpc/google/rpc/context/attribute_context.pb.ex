defmodule Google.Rpc.Context.AttributeContext.Peer.LabelsEntry do
  @moduledoc false

  use Protobuf, map: true, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Google.Rpc.Context.AttributeContext.Peer do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :ip, 1, type: :string
  field :port, 2, type: :int64

  field :labels, 6,
    repeated: true,
    type: Google.Rpc.Context.AttributeContext.Peer.LabelsEntry,
    map: true

  field :principal, 7, type: :string
  field :region_code, 8, type: :string, json_name: "regionCode"
end

defmodule Google.Rpc.Context.AttributeContext.Api do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :service, 1, type: :string
  field :operation, 2, type: :string
  field :protocol, 3, type: :string
  field :version, 4, type: :string
end

defmodule Google.Rpc.Context.AttributeContext.Auth do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :principal, 1, type: :string
  field :audiences, 2, repeated: true, type: :string
  field :presenter, 3, type: :string
  field :claims, 4, type: Google.Protobuf.Struct
  field :access_levels, 5, repeated: true, type: :string, json_name: "accessLevels"
end

defmodule Google.Rpc.Context.AttributeContext.Request.HeadersEntry do
  @moduledoc false

  use Protobuf, map: true, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Google.Rpc.Context.AttributeContext.Request do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :id, 1, type: :string
  field :method, 2, type: :string

  field :headers, 3,
    repeated: true,
    type: Google.Rpc.Context.AttributeContext.Request.HeadersEntry,
    map: true

  field :path, 4, type: :string
  field :host, 5, type: :string
  field :scheme, 6, type: :string
  field :query, 7, type: :string
  field :time, 9, type: Google.Protobuf.Timestamp
  field :size, 10, type: :int64
  field :protocol, 11, type: :string
  field :reason, 12, type: :string
  field :auth, 13, type: Google.Rpc.Context.AttributeContext.Auth
end

defmodule Google.Rpc.Context.AttributeContext.Response.HeadersEntry do
  @moduledoc false

  use Protobuf, map: true, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Google.Rpc.Context.AttributeContext.Response do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :code, 1, type: :int64
  field :size, 2, type: :int64

  field :headers, 3,
    repeated: true,
    type: Google.Rpc.Context.AttributeContext.Response.HeadersEntry,
    map: true

  field :time, 4, type: Google.Protobuf.Timestamp
  field :backend_latency, 5, type: Google.Protobuf.Duration, json_name: "backendLatency"
end

defmodule Google.Rpc.Context.AttributeContext.Resource.LabelsEntry do
  @moduledoc false

  use Protobuf, map: true, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Google.Rpc.Context.AttributeContext.Resource.AnnotationsEntry do
  @moduledoc false

  use Protobuf, map: true, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Google.Rpc.Context.AttributeContext.Resource do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :service, 1, type: :string
  field :name, 2, type: :string
  field :type, 3, type: :string

  field :labels, 4,
    repeated: true,
    type: Google.Rpc.Context.AttributeContext.Resource.LabelsEntry,
    map: true

  field :uid, 5, type: :string

  field :annotations, 6,
    repeated: true,
    type: Google.Rpc.Context.AttributeContext.Resource.AnnotationsEntry,
    map: true

  field :display_name, 7, type: :string, json_name: "displayName"
  field :create_time, 8, type: Google.Protobuf.Timestamp, json_name: "createTime"
  field :update_time, 9, type: Google.Protobuf.Timestamp, json_name: "updateTime"
  field :delete_time, 10, type: Google.Protobuf.Timestamp, json_name: "deleteTime"
  field :etag, 11, type: :string
  field :location, 12, type: :string
end

defmodule Google.Rpc.Context.AttributeContext do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :origin, 7, type: Google.Rpc.Context.AttributeContext.Peer
  field :source, 1, type: Google.Rpc.Context.AttributeContext.Peer
  field :destination, 2, type: Google.Rpc.Context.AttributeContext.Peer
  field :request, 3, type: Google.Rpc.Context.AttributeContext.Request
  field :response, 4, type: Google.Rpc.Context.AttributeContext.Response
  field :resource, 5, type: Google.Rpc.Context.AttributeContext.Resource
  field :api, 6, type: Google.Rpc.Context.AttributeContext.Api
  field :extensions, 8, repeated: true, type: Google.Protobuf.Any
end
