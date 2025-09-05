defmodule Google.Rpc.HttpRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :method, 1, type: :string
  field :uri, 2, type: :string
  field :headers, 3, repeated: true, type: Google.Rpc.HttpHeader
  field :body, 4, type: :bytes
end

defmodule Google.Rpc.HttpResponse do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :status, 1, type: :int32
  field :reason, 2, type: :string
  field :headers, 3, repeated: true, type: Google.Rpc.HttpHeader
  field :body, 4, type: :bytes
end

defmodule Google.Rpc.HttpHeader do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :key, 1, type: :string
  field :value, 2, type: :string
end
