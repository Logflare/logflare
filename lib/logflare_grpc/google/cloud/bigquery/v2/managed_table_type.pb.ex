defmodule Google.Cloud.Bigquery.V2.ManagedTableType do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :MANAGED_TABLE_TYPE_UNSPECIFIED, 0
  field :NATIVE, 1
  field :ICEBERG, 2
end
