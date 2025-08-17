defmodule Google.Cloud.Bigquery.Storage.V1beta2.StorageError.StorageErrorCode do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :STORAGE_ERROR_CODE_UNSPECIFIED, 0
  field :TABLE_NOT_FOUND, 1
  field :STREAM_ALREADY_COMMITTED, 2
  field :STREAM_NOT_FOUND, 3
  field :INVALID_STREAM_TYPE, 4
  field :INVALID_STREAM_STATE, 5
  field :STREAM_FINALIZED, 6
end

defmodule Google.Cloud.Bigquery.Storage.V1beta2.CreateReadSessionRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false

  field :read_session, 2,
    type: Google.Cloud.Bigquery.Storage.V1beta2.ReadSession,
    json_name: "readSession",
    deprecated: false

  field :max_stream_count, 3, type: :int32, json_name: "maxStreamCount"
end

defmodule Google.Cloud.Bigquery.Storage.V1beta2.ReadRowsRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :read_stream, 1, type: :string, json_name: "readStream", deprecated: false
  field :offset, 2, type: :int64
end

defmodule Google.Cloud.Bigquery.Storage.V1beta2.ThrottleState do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :throttle_percent, 1, type: :int32, json_name: "throttlePercent"
end

defmodule Google.Cloud.Bigquery.Storage.V1beta2.StreamStats.Progress do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :at_response_start, 1, type: :double, json_name: "atResponseStart"
  field :at_response_end, 2, type: :double, json_name: "atResponseEnd"
end

defmodule Google.Cloud.Bigquery.Storage.V1beta2.StreamStats do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :progress, 2, type: Google.Cloud.Bigquery.Storage.V1beta2.StreamStats.Progress
end

defmodule Google.Cloud.Bigquery.Storage.V1beta2.ReadRowsResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:rows, 0)

  oneof(:schema, 1)

  field :avro_rows, 3,
    type: Google.Cloud.Bigquery.Storage.V1beta2.AvroRows,
    json_name: "avroRows",
    oneof: 0

  field :arrow_record_batch, 4,
    type: Google.Cloud.Bigquery.Storage.V1beta2.ArrowRecordBatch,
    json_name: "arrowRecordBatch",
    oneof: 0

  field :row_count, 6, type: :int64, json_name: "rowCount"
  field :stats, 2, type: Google.Cloud.Bigquery.Storage.V1beta2.StreamStats

  field :throttle_state, 5,
    type: Google.Cloud.Bigquery.Storage.V1beta2.ThrottleState,
    json_name: "throttleState"

  field :avro_schema, 7,
    type: Google.Cloud.Bigquery.Storage.V1beta2.AvroSchema,
    json_name: "avroSchema",
    oneof: 1,
    deprecated: false

  field :arrow_schema, 8,
    type: Google.Cloud.Bigquery.Storage.V1beta2.ArrowSchema,
    json_name: "arrowSchema",
    oneof: 1,
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1beta2.SplitReadStreamRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
  field :fraction, 2, type: :double
end

defmodule Google.Cloud.Bigquery.Storage.V1beta2.SplitReadStreamResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :primary_stream, 1,
    type: Google.Cloud.Bigquery.Storage.V1beta2.ReadStream,
    json_name: "primaryStream"

  field :remainder_stream, 2,
    type: Google.Cloud.Bigquery.Storage.V1beta2.ReadStream,
    json_name: "remainderStream"
end

defmodule Google.Cloud.Bigquery.Storage.V1beta2.CreateWriteStreamRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false

  field :write_stream, 2,
    type: Google.Cloud.Bigquery.Storage.V1beta2.WriteStream,
    json_name: "writeStream",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1beta2.AppendRowsRequest.ProtoData do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :writer_schema, 1,
    type: Google.Cloud.Bigquery.Storage.V1beta2.ProtoSchema,
    json_name: "writerSchema"

  field :rows, 2, type: Google.Cloud.Bigquery.Storage.V1beta2.ProtoRows
end

defmodule Google.Cloud.Bigquery.Storage.V1beta2.AppendRowsRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:rows, 0)

  field :write_stream, 1, type: :string, json_name: "writeStream", deprecated: false
  field :offset, 2, type: Google.Protobuf.Int64Value

  field :proto_rows, 4,
    type: Google.Cloud.Bigquery.Storage.V1beta2.AppendRowsRequest.ProtoData,
    json_name: "protoRows",
    oneof: 0

  field :trace_id, 6, type: :string, json_name: "traceId"
end

defmodule Google.Cloud.Bigquery.Storage.V1beta2.AppendRowsResponse.AppendResult do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :offset, 1, type: Google.Protobuf.Int64Value
end

defmodule Google.Cloud.Bigquery.Storage.V1beta2.AppendRowsResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:response, 0)

  field :append_result, 1,
    type: Google.Cloud.Bigquery.Storage.V1beta2.AppendRowsResponse.AppendResult,
    json_name: "appendResult",
    oneof: 0

  field :error, 2, type: Google.Rpc.Status, oneof: 0

  field :updated_schema, 3,
    type: Google.Cloud.Bigquery.Storage.V1beta2.TableSchema,
    json_name: "updatedSchema"
end

defmodule Google.Cloud.Bigquery.Storage.V1beta2.GetWriteStreamRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1beta2.BatchCommitWriteStreamsRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false

  field :write_streams, 2,
    repeated: true,
    type: :string,
    json_name: "writeStreams",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1beta2.BatchCommitWriteStreamsResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :commit_time, 1, type: Google.Protobuf.Timestamp, json_name: "commitTime"

  field :stream_errors, 2,
    repeated: true,
    type: Google.Cloud.Bigquery.Storage.V1beta2.StorageError,
    json_name: "streamErrors"
end

defmodule Google.Cloud.Bigquery.Storage.V1beta2.FinalizeWriteStreamRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1beta2.FinalizeWriteStreamResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :row_count, 1, type: :int64, json_name: "rowCount"
end

defmodule Google.Cloud.Bigquery.Storage.V1beta2.FlushRowsRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :write_stream, 1, type: :string, json_name: "writeStream", deprecated: false
  field :offset, 2, type: Google.Protobuf.Int64Value
end

defmodule Google.Cloud.Bigquery.Storage.V1beta2.FlushRowsResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :offset, 1, type: :int64
end

defmodule Google.Cloud.Bigquery.Storage.V1beta2.StorageError do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :code, 1,
    type: Google.Cloud.Bigquery.Storage.V1beta2.StorageError.StorageErrorCode,
    enum: true

  field :entity, 2, type: :string
  field :error_message, 3, type: :string, json_name: "errorMessage"
end

defmodule Google.Cloud.Bigquery.Storage.V1beta2.BigQueryRead.Service do
  @moduledoc false

  use GRPC.Service,
    name: "google.cloud.bigquery.storage.v1beta2.BigQueryRead",
    protoc_gen_elixir_version: "0.13.0"

  rpc(
    :CreateReadSession,
    Google.Cloud.Bigquery.Storage.V1beta2.CreateReadSessionRequest,
    Google.Cloud.Bigquery.Storage.V1beta2.ReadSession
  )

  rpc(
    :ReadRows,
    Google.Cloud.Bigquery.Storage.V1beta2.ReadRowsRequest,
    stream(Google.Cloud.Bigquery.Storage.V1beta2.ReadRowsResponse)
  )

  rpc(
    :SplitReadStream,
    Google.Cloud.Bigquery.Storage.V1beta2.SplitReadStreamRequest,
    Google.Cloud.Bigquery.Storage.V1beta2.SplitReadStreamResponse
  )
end

defmodule Google.Cloud.Bigquery.Storage.V1beta2.BigQueryRead.Stub do
  @moduledoc false

  use GRPC.Stub, service: Google.Cloud.Bigquery.Storage.V1beta2.BigQueryRead.Service
end

defmodule Google.Cloud.Bigquery.Storage.V1beta2.BigQueryWrite.Service do
  @moduledoc false

  use GRPC.Service,
    name: "google.cloud.bigquery.storage.v1beta2.BigQueryWrite",
    protoc_gen_elixir_version: "0.13.0"

  rpc(
    :CreateWriteStream,
    Google.Cloud.Bigquery.Storage.V1beta2.CreateWriteStreamRequest,
    Google.Cloud.Bigquery.Storage.V1beta2.WriteStream
  )

  rpc(
    :AppendRows,
    stream(Google.Cloud.Bigquery.Storage.V1beta2.AppendRowsRequest),
    stream(Google.Cloud.Bigquery.Storage.V1beta2.AppendRowsResponse)
  )

  rpc(
    :GetWriteStream,
    Google.Cloud.Bigquery.Storage.V1beta2.GetWriteStreamRequest,
    Google.Cloud.Bigquery.Storage.V1beta2.WriteStream
  )

  rpc(
    :FinalizeWriteStream,
    Google.Cloud.Bigquery.Storage.V1beta2.FinalizeWriteStreamRequest,
    Google.Cloud.Bigquery.Storage.V1beta2.FinalizeWriteStreamResponse
  )

  rpc(
    :BatchCommitWriteStreams,
    Google.Cloud.Bigquery.Storage.V1beta2.BatchCommitWriteStreamsRequest,
    Google.Cloud.Bigquery.Storage.V1beta2.BatchCommitWriteStreamsResponse
  )

  rpc(
    :FlushRows,
    Google.Cloud.Bigquery.Storage.V1beta2.FlushRowsRequest,
    Google.Cloud.Bigquery.Storage.V1beta2.FlushRowsResponse
  )
end

defmodule Google.Cloud.Bigquery.Storage.V1beta2.BigQueryWrite.Stub do
  @moduledoc false

  use GRPC.Stub, service: Google.Cloud.Bigquery.Storage.V1beta2.BigQueryWrite.Service
end
