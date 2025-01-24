defmodule Google.Api.PbExtension do
  @moduledoc false
  use Protobuf, protoc_gen_elixir_version: "0.13.0"

  extend Google.Protobuf.MethodOptions, :http, 72_295_728,
    optional: true,
    type: Google.Api.HttpRule
end