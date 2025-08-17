defmodule Google.Cloud.Bigquery.Storage.V1alpha.CreateMetastorePartitionRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false

  field :metastore_partition, 2,
    type: Google.Cloud.Bigquery.Storage.V1alpha.MetastorePartition,
    json_name: "metastorePartition",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1alpha.BatchCreateMetastorePartitionsRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false

  field :requests, 2,
    repeated: true,
    type: Google.Cloud.Bigquery.Storage.V1alpha.CreateMetastorePartitionRequest,
    deprecated: false

  field :skip_existing_partitions, 3,
    type: :bool,
    json_name: "skipExistingPartitions",
    deprecated: false

  field :trace_id, 4, type: :string, json_name: "traceId", deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1alpha.BatchCreateMetastorePartitionsResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :partitions, 1,
    repeated: true,
    type: Google.Cloud.Bigquery.Storage.V1alpha.MetastorePartition
end

defmodule Google.Cloud.Bigquery.Storage.V1alpha.BatchDeleteMetastorePartitionsRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false

  field :partition_values, 2,
    repeated: true,
    type: Google.Cloud.Bigquery.Storage.V1alpha.MetastorePartitionValues,
    json_name: "partitionValues",
    deprecated: false

  field :trace_id, 4, type: :string, json_name: "traceId", deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1alpha.UpdateMetastorePartitionRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :metastore_partition, 1,
    type: Google.Cloud.Bigquery.Storage.V1alpha.MetastorePartition,
    json_name: "metastorePartition",
    deprecated: false

  field :update_mask, 2,
    type: Google.Protobuf.FieldMask,
    json_name: "updateMask",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1alpha.BatchUpdateMetastorePartitionsRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false

  field :requests, 2,
    repeated: true,
    type: Google.Cloud.Bigquery.Storage.V1alpha.UpdateMetastorePartitionRequest,
    deprecated: false

  field :trace_id, 4, type: :string, json_name: "traceId", deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1alpha.BatchUpdateMetastorePartitionsResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :partitions, 1,
    repeated: true,
    type: Google.Cloud.Bigquery.Storage.V1alpha.MetastorePartition
end

defmodule Google.Cloud.Bigquery.Storage.V1alpha.ListMetastorePartitionsRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false
  field :filter, 2, type: :string, deprecated: false
  field :trace_id, 3, type: :string, json_name: "traceId", deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1alpha.ListMetastorePartitionsResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:response, 0)

  field :partitions, 1,
    type: Google.Cloud.Bigquery.Storage.V1alpha.MetastorePartitionList,
    oneof: 0

  field :streams, 2, type: Google.Cloud.Bigquery.Storage.V1alpha.StreamList, oneof: 0
end

defmodule Google.Cloud.Bigquery.Storage.V1alpha.StreamMetastorePartitionsRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :parent, 1, type: :string, deprecated: false

  field :metastore_partitions, 2,
    repeated: true,
    type: Google.Cloud.Bigquery.Storage.V1alpha.MetastorePartition,
    json_name: "metastorePartitions",
    deprecated: false

  field :skip_existing_partitions, 3,
    type: :bool,
    json_name: "skipExistingPartitions",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1alpha.StreamMetastorePartitionsResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :total_partitions_streamed_count, 2,
    type: :int64,
    json_name: "totalPartitionsStreamedCount"

  field :total_partitions_inserted_count, 3,
    type: :int64,
    json_name: "totalPartitionsInsertedCount"
end

defmodule Google.Cloud.Bigquery.Storage.V1alpha.BatchSizeTooLargeError do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :max_batch_size, 1, type: :int64, json_name: "maxBatchSize"
  field :error_message, 2, type: :string, json_name: "errorMessage", deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1alpha.MetastorePartitionService.Service do
  @moduledoc false

  use GRPC.Service,
    name: "google.cloud.bigquery.storage.v1alpha.MetastorePartitionService",
    protoc_gen_elixir_version: "0.13.0"

  rpc(
    :BatchCreateMetastorePartitions,
    Google.Cloud.Bigquery.Storage.V1alpha.BatchCreateMetastorePartitionsRequest,
    Google.Cloud.Bigquery.Storage.V1alpha.BatchCreateMetastorePartitionsResponse
  )

  rpc(
    :BatchDeleteMetastorePartitions,
    Google.Cloud.Bigquery.Storage.V1alpha.BatchDeleteMetastorePartitionsRequest,
    Google.Protobuf.Empty
  )

  rpc(
    :BatchUpdateMetastorePartitions,
    Google.Cloud.Bigquery.Storage.V1alpha.BatchUpdateMetastorePartitionsRequest,
    Google.Cloud.Bigquery.Storage.V1alpha.BatchUpdateMetastorePartitionsResponse
  )

  rpc(
    :ListMetastorePartitions,
    Google.Cloud.Bigquery.Storage.V1alpha.ListMetastorePartitionsRequest,
    Google.Cloud.Bigquery.Storage.V1alpha.ListMetastorePartitionsResponse
  )

  rpc(
    :StreamMetastorePartitions,
    stream(Google.Cloud.Bigquery.Storage.V1alpha.StreamMetastorePartitionsRequest),
    stream(Google.Cloud.Bigquery.Storage.V1alpha.StreamMetastorePartitionsResponse)
  )
end

defmodule Google.Cloud.Bigquery.Storage.V1alpha.MetastorePartitionService.Stub do
  @moduledoc false

  use GRPC.Stub, service: Google.Cloud.Bigquery.Storage.V1alpha.MetastorePartitionService.Service
end
