defmodule Google.Cloud.Bigquery.Storage.V1.PbExtension do
  @moduledoc false
  use Protobuf, protoc_gen_elixir_version: "0.13.0"

  extend(Google.Protobuf.FieldOptions, :column_name, 454_943_157,
    optional: true,
    type: :string,
    json_name: "columnName"
  )
end
