defmodule Google.Cloud.Bigquery.V2.TimePartitioning do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :type, 1, type: :string, deprecated: false

  field :expiration_ms, 2,
    type: Google.Protobuf.Int64Value,
    json_name: "expirationMs",
    deprecated: false

  field :field, 3, type: Google.Protobuf.StringValue, deprecated: false
end
