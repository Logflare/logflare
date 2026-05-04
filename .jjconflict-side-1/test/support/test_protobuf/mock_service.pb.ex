defmodule Logflare.TestProtobuf.Mock.EmptyRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"
end

defmodule Logflare.TestProtobuf.Mock.EmptyResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"
end

defmodule Logflare.TestProtobuf.Mock.MockService.Service do
  @moduledoc false

  use GRPC.Service,
    name: "logflare.test_protobuf.mock.MockService",
    protoc_gen_elixir_version: "0.13.0"

  rpc(
    :DoNothing,
    Logflare.TestProtobuf.Mock.EmptyRequest,
    Logflare.TestProtobuf.Mock.EmptyResponse
  )
end

defmodule Logflare.TestProtobuf.Mock.MockService.Stub do
  @moduledoc false

  use GRPC.Stub, service: Logflare.TestProtobuf.Mock.MockService.Service
end
