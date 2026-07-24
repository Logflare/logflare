defmodule Vector.ServingStatus do
  @moduledoc false

  use Protobuf, enum: true, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :SERVING, 0
  field :NOT_SERVING, 1
end

defmodule Vector.PushEventsRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :events, 1, repeated: true, type: Event.EventWrapper
end

defmodule Vector.PushEventsResponse do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3
end

defmodule Vector.HealthCheckRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3
end

defmodule Vector.HealthCheckResponse do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.15.0", syntax: :proto3

  field :status, 1, type: Vector.ServingStatus, enum: true
end

defmodule Vector.Vector.Service do
  @moduledoc false

  use GRPC.Service, name: "vector.Vector", protoc_gen_elixir_version: "0.15.0"

  rpc(:PushEvents, Vector.PushEventsRequest, Vector.PushEventsResponse)

  rpc(:HealthCheck, Vector.HealthCheckRequest, Vector.HealthCheckResponse)
end

defmodule Vector.Vector.Stub do
  @moduledoc false

  use GRPC.Stub, service: Vector.Vector.Service
end
