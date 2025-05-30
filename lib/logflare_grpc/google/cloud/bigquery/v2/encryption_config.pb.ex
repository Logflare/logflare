defmodule Google.Cloud.Bigquery.V2.EncryptionConfiguration do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :kms_key_name, 1,
    type: Google.Protobuf.StringValue,
    json_name: "kmsKeyName",
    deprecated: false
end
