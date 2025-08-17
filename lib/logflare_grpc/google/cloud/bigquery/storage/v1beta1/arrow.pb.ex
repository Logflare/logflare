defmodule Google.Cloud.Bigquery.Storage.V1beta1.ArrowSchema do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :serialized_schema, 1, type: :bytes, json_name: "serializedSchema"
end

defmodule Google.Cloud.Bigquery.Storage.V1beta1.ArrowRecordBatch do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :serialized_record_batch, 1, type: :bytes, json_name: "serializedRecordBatch"
  field :row_count, 2, type: :int64, json_name: "rowCount"
end
