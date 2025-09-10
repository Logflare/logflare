defmodule Google.Cloud.Bigquery.Storage.V1.ArrowSerializationOptions.CompressionCodec do
  @moduledoc false

  use Protobuf, enum: true, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :COMPRESSION_UNSPECIFIED, 0
  field :LZ4_FRAME, 1
  field :ZSTD, 2
end

defmodule Google.Cloud.Bigquery.Storage.V1.ArrowSchema do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :serialized_schema, 1, type: :bytes, json_name: "serializedSchema"
end

defmodule Google.Cloud.Bigquery.Storage.V1.ArrowRecordBatch do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :serialized_record_batch, 1, type: :bytes, json_name: "serializedRecordBatch"
  field :row_count, 2, type: :int64, json_name: "rowCount", deprecated: true
end

defmodule Google.Cloud.Bigquery.Storage.V1.ArrowSerializationOptions do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :buffer_compression, 2,
    type: Google.Cloud.Bigquery.Storage.V1.ArrowSerializationOptions.CompressionCodec,
    json_name: "bufferCompression",
    enum: true
end
