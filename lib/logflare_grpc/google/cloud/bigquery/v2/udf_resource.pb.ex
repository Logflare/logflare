defmodule Google.Cloud.Bigquery.V2.UserDefinedFunctionResource do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :resource_uri, 1, type: Google.Protobuf.StringValue, json_name: "resourceUri"
  field :inline_code, 2, type: Google.Protobuf.StringValue, json_name: "inlineCode"
end
