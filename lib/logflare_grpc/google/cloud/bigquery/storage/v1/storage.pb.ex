defmodule Google.Cloud.Bigquery.Storage.V1.AppendRowsRequest.MissingValueInterpretation do
  @moduledoc false

  use Protobuf, enum: true, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :MISSING_VALUE_INTERPRETATION_UNSPECIFIED, 0
  field :NULL_VALUE, 1
  field :DEFAULT_VALUE, 2
end

defmodule Google.Cloud.Bigquery.Storage.V1.StorageError.StorageErrorCode do
  @moduledoc false

  use Protobuf, enum: true, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :STORAGE_ERROR_CODE_UNSPECIFIED, 0
  field :TABLE_NOT_FOUND, 1
  field :STREAM_ALREADY_COMMITTED, 2
  field :STREAM_NOT_FOUND, 3
  field :INVALID_STREAM_TYPE, 4
  field :INVALID_STREAM_STATE, 5
  field :STREAM_FINALIZED, 6
  field :SCHEMA_MISMATCH_EXTRA_FIELDS, 7
  field :OFFSET_ALREADY_EXISTS, 8
  field :OFFSET_OUT_OF_RANGE, 9
  field :CMEK_NOT_PROVIDED, 10
  field :INVALID_CMEK_PROVIDED, 11
  field :CMEK_ENCRYPTION_ERROR, 12
  field :KMS_SERVICE_ERROR, 13
  field :KMS_PERMISSION_DENIED, 14
end

defmodule Google.Cloud.Bigquery.Storage.V1.RowError.RowErrorCode do
  @moduledoc false

  use Protobuf, enum: true, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :ROW_ERROR_CODE_UNSPECIFIED, 0
  field :FIELDS_ERROR, 1
end

defmodule Google.Cloud.Bigquery.Storage.V1.CreateReadSessionRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :parent, 1, type: :string, deprecated: false

  field :read_session, 2,
    type: Google.Cloud.Bigquery.Storage.V1.ReadSession,
    json_name: "readSession",
    deprecated: false

  field :max_stream_count, 3, type: :int32, json_name: "maxStreamCount"
  field :preferred_min_stream_count, 4, type: :int32, json_name: "preferredMinStreamCount"
end

defmodule Google.Cloud.Bigquery.Storage.V1.ReadRowsRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :read_stream, 1, type: :string, json_name: "readStream", deprecated: false
  field :offset, 2, type: :int64
end

defmodule Google.Cloud.Bigquery.Storage.V1.ThrottleState do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :throttle_percent, 1, type: :int32, json_name: "throttlePercent"
end

defmodule Google.Cloud.Bigquery.Storage.V1.StreamStats.Progress do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :at_response_start, 1, type: :double, json_name: "atResponseStart"
  field :at_response_end, 2, type: :double, json_name: "atResponseEnd"
end

defmodule Google.Cloud.Bigquery.Storage.V1.StreamStats do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :progress, 2, type: Google.Cloud.Bigquery.Storage.V1.StreamStats.Progress
end

defmodule Google.Cloud.Bigquery.Storage.V1.ReadRowsResponse do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  oneof(:rows, 0)

  oneof(:schema, 1)

  field :avro_rows, 3,
    type: Google.Cloud.Bigquery.Storage.V1.AvroRows,
    json_name: "avroRows",
    oneof: 0

  field :arrow_record_batch, 4,
    type: Google.Cloud.Bigquery.Storage.V1.ArrowRecordBatch,
    json_name: "arrowRecordBatch",
    oneof: 0

  field :row_count, 6, type: :int64, json_name: "rowCount"
  field :stats, 2, type: Google.Cloud.Bigquery.Storage.V1.StreamStats

  field :throttle_state, 5,
    type: Google.Cloud.Bigquery.Storage.V1.ThrottleState,
    json_name: "throttleState"

  field :avro_schema, 7,
    type: Google.Cloud.Bigquery.Storage.V1.AvroSchema,
    json_name: "avroSchema",
    oneof: 1,
    deprecated: false

  field :arrow_schema, 8,
    type: Google.Cloud.Bigquery.Storage.V1.ArrowSchema,
    json_name: "arrowSchema",
    oneof: 1,
    deprecated: false

  field :uncompressed_byte_size, 9,
    proto3_optional: true,
    type: :int64,
    json_name: "uncompressedByteSize",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1.SplitReadStreamRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :name, 1, type: :string, deprecated: false
  field :fraction, 2, type: :double
end

defmodule Google.Cloud.Bigquery.Storage.V1.SplitReadStreamResponse do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :primary_stream, 1,
    type: Google.Cloud.Bigquery.Storage.V1.ReadStream,
    json_name: "primaryStream"

  field :remainder_stream, 2,
    type: Google.Cloud.Bigquery.Storage.V1.ReadStream,
    json_name: "remainderStream"
end

defmodule Google.Cloud.Bigquery.Storage.V1.CreateWriteStreamRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :parent, 1, type: :string, deprecated: false

  field :write_stream, 2,
    type: Google.Cloud.Bigquery.Storage.V1.WriteStream,
    json_name: "writeStream",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1.AppendRowsRequest.ArrowData do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :writer_schema, 1,
    type: Google.Cloud.Bigquery.Storage.V1.ArrowSchema,
    json_name: "writerSchema"

  field :rows, 2, type: Google.Cloud.Bigquery.Storage.V1.ArrowRecordBatch
end

defmodule Google.Cloud.Bigquery.Storage.V1.AppendRowsRequest.ProtoData do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :writer_schema, 1,
    type: Google.Cloud.Bigquery.Storage.V1.ProtoSchema,
    json_name: "writerSchema"

  field :rows, 2, type: Google.Cloud.Bigquery.Storage.V1.ProtoRows
end

defmodule Google.Cloud.Bigquery.Storage.V1.AppendRowsRequest.MissingValueInterpretationsEntry do
  @moduledoc false

  use Protobuf, map: true, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :key, 1, type: :string

  field :value, 2,
    type: Google.Cloud.Bigquery.Storage.V1.AppendRowsRequest.MissingValueInterpretation,
    enum: true
end

defmodule Google.Cloud.Bigquery.Storage.V1.AppendRowsRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  oneof(:rows, 0)

  field :write_stream, 1, type: :string, json_name: "writeStream", deprecated: false
  field :offset, 2, type: Google.Protobuf.Int64Value

  field :proto_rows, 4,
    type: Google.Cloud.Bigquery.Storage.V1.AppendRowsRequest.ProtoData,
    json_name: "protoRows",
    oneof: 0

  field :arrow_rows, 5,
    type: Google.Cloud.Bigquery.Storage.V1.AppendRowsRequest.ArrowData,
    json_name: "arrowRows",
    oneof: 0

  field :trace_id, 6, type: :string, json_name: "traceId"

  field :missing_value_interpretations, 7,
    repeated: true,
    type: Google.Cloud.Bigquery.Storage.V1.AppendRowsRequest.MissingValueInterpretationsEntry,
    json_name: "missingValueInterpretations",
    map: true

  field :default_missing_value_interpretation, 8,
    type: Google.Cloud.Bigquery.Storage.V1.AppendRowsRequest.MissingValueInterpretation,
    json_name: "defaultMissingValueInterpretation",
    enum: true,
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1.AppendRowsResponse.AppendResult do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :offset, 1, type: Google.Protobuf.Int64Value
end

defmodule Google.Cloud.Bigquery.Storage.V1.AppendRowsResponse do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  oneof(:response, 0)

  field :append_result, 1,
    type: Google.Cloud.Bigquery.Storage.V1.AppendRowsResponse.AppendResult,
    json_name: "appendResult",
    oneof: 0

  field :error, 2, type: Google.Rpc.Status, oneof: 0

  field :updated_schema, 3,
    type: Google.Cloud.Bigquery.Storage.V1.TableSchema,
    json_name: "updatedSchema"

  field :row_errors, 4,
    repeated: true,
    type: Google.Cloud.Bigquery.Storage.V1.RowError,
    json_name: "rowErrors"

  field :write_stream, 5, type: :string, json_name: "writeStream"
end

defmodule Google.Cloud.Bigquery.Storage.V1.GetWriteStreamRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :name, 1, type: :string, deprecated: false
  field :view, 3, type: Google.Cloud.Bigquery.Storage.V1.WriteStreamView, enum: true
end

defmodule Google.Cloud.Bigquery.Storage.V1.BatchCommitWriteStreamsRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :parent, 1, type: :string, deprecated: false

  field :write_streams, 2,
    repeated: true,
    type: :string,
    json_name: "writeStreams",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1.BatchCommitWriteStreamsResponse do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :commit_time, 1, type: Google.Protobuf.Timestamp, json_name: "commitTime"

  field :stream_errors, 2,
    repeated: true,
    type: Google.Cloud.Bigquery.Storage.V1.StorageError,
    json_name: "streamErrors"
end

defmodule Google.Cloud.Bigquery.Storage.V1.FinalizeWriteStreamRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1.FinalizeWriteStreamResponse do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :row_count, 1, type: :int64, json_name: "rowCount"
end

defmodule Google.Cloud.Bigquery.Storage.V1.FlushRowsRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :write_stream, 1, type: :string, json_name: "writeStream", deprecated: false
  field :offset, 2, type: Google.Protobuf.Int64Value
end

defmodule Google.Cloud.Bigquery.Storage.V1.FlushRowsResponse do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :offset, 1, type: :int64
end

defmodule Google.Cloud.Bigquery.Storage.V1.StorageError do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :code, 1, type: Google.Cloud.Bigquery.Storage.V1.StorageError.StorageErrorCode, enum: true
  field :entity, 2, type: :string
  field :error_message, 3, type: :string, json_name: "errorMessage"
end

defmodule Google.Cloud.Bigquery.Storage.V1.RowError do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :index, 1, type: :int64
  field :code, 2, type: Google.Cloud.Bigquery.Storage.V1.RowError.RowErrorCode, enum: true
  field :message, 3, type: :string
end

defmodule Google.Cloud.Bigquery.Storage.V1.BigQueryRead.Service do
  @moduledoc false

  use GRPC.Service,
    name: "google.cloud.bigquery.storage.v1.BigQueryRead",
    protoc_gen_elixir_version: "0.14.1"

  rpc(
    :CreateReadSession,
    Google.Cloud.Bigquery.Storage.V1.CreateReadSessionRequest,
    Google.Cloud.Bigquery.Storage.V1.ReadSession
  )

  rpc(
    :ReadRows,
    Google.Cloud.Bigquery.Storage.V1.ReadRowsRequest,
    stream(Google.Cloud.Bigquery.Storage.V1.ReadRowsResponse)
  )

  rpc(
    :SplitReadStream,
    Google.Cloud.Bigquery.Storage.V1.SplitReadStreamRequest,
    Google.Cloud.Bigquery.Storage.V1.SplitReadStreamResponse
  )
end

defmodule Google.Cloud.Bigquery.Storage.V1.BigQueryRead.Stub do
  @moduledoc false

  use GRPC.Stub, service: Google.Cloud.Bigquery.Storage.V1.BigQueryRead.Service
end

defmodule Google.Cloud.Bigquery.Storage.V1.BigQueryWrite.Service do
  @moduledoc false

  use GRPC.Service,
    name: "google.cloud.bigquery.storage.v1.BigQueryWrite",
    protoc_gen_elixir_version: "0.14.1"

  rpc(
    :CreateWriteStream,
    Google.Cloud.Bigquery.Storage.V1.CreateWriteStreamRequest,
    Google.Cloud.Bigquery.Storage.V1.WriteStream
  )

  rpc(
    :AppendRows,
    stream(Google.Cloud.Bigquery.Storage.V1.AppendRowsRequest),
    stream(Google.Cloud.Bigquery.Storage.V1.AppendRowsResponse)
  )

  rpc(
    :GetWriteStream,
    Google.Cloud.Bigquery.Storage.V1.GetWriteStreamRequest,
    Google.Cloud.Bigquery.Storage.V1.WriteStream
  )

  rpc(
    :FinalizeWriteStream,
    Google.Cloud.Bigquery.Storage.V1.FinalizeWriteStreamRequest,
    Google.Cloud.Bigquery.Storage.V1.FinalizeWriteStreamResponse
  )

  rpc(
    :BatchCommitWriteStreams,
    Google.Cloud.Bigquery.Storage.V1.BatchCommitWriteStreamsRequest,
    Google.Cloud.Bigquery.Storage.V1.BatchCommitWriteStreamsResponse
  )

  rpc(
    :FlushRows,
    Google.Cloud.Bigquery.Storage.V1.FlushRowsRequest,
    Google.Cloud.Bigquery.Storage.V1.FlushRowsResponse
  )
end

defmodule Google.Cloud.Bigquery.Storage.V1.BigQueryWrite.Stub do
  @moduledoc false

  use GRPC.Stub, service: Google.Cloud.Bigquery.Storage.V1.BigQueryWrite.Service
end
