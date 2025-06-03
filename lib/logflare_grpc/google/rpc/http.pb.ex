defmodule Google.Rpc.HttpRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :method, 1, type: :string
  field :uri, 2, type: :string
  field :headers, 3, repeated: true, type: Google.Rpc.HttpHeader
  field :body, 4, type: :bytes
end

defmodule Google.Rpc.HttpResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :status, 1, type: :int32
  field :reason, 2, type: :string
  field :headers, 3, repeated: true, type: Google.Rpc.HttpHeader
  field :body, 4, type: :bytes
end

defmodule Google.Rpc.HttpHeader do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :key, 1, type: :string
  field :value, 2, type: :string
end
