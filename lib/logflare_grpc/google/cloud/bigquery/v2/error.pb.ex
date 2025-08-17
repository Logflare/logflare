defmodule Google.Cloud.Bigquery.V2.ErrorProto do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :reason, 1, type: :string
  field :location, 2, type: :string
  field :debug_info, 3, type: :string, json_name: "debugInfo"
  field :message, 4, type: :string
end
