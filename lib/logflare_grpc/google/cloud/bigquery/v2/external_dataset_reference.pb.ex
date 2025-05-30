defmodule Google.Cloud.Bigquery.V2.ExternalDatasetReference do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :external_source, 2, type: :string, json_name: "externalSource", deprecated: false
  field :connection, 3, type: :string, deprecated: false
end
