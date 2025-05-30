defmodule Google.Cloud.Bigquery.Storage.V1beta2.DataFormat do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :DATA_FORMAT_UNSPECIFIED, 0
  field :AVRO, 1
  field :ARROW, 2
end

defmodule Google.Cloud.Bigquery.Storage.V1beta2.WriteStream.Type do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :TYPE_UNSPECIFIED, 0
  field :COMMITTED, 1
  field :PENDING, 2
  field :BUFFERED, 3
end

defmodule Google.Cloud.Bigquery.Storage.V1beta2.ReadSession.TableModifiers do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :snapshot_time, 1, type: Google.Protobuf.Timestamp, json_name: "snapshotTime"
end

defmodule Google.Cloud.Bigquery.Storage.V1beta2.ReadSession.TableReadOptions do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :selected_fields, 1, repeated: true, type: :string, json_name: "selectedFields"
  field :row_restriction, 2, type: :string, json_name: "rowRestriction"

  field :arrow_serialization_options, 3,
    type: Google.Cloud.Bigquery.Storage.V1beta2.ArrowSerializationOptions,
    json_name: "arrowSerializationOptions",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1beta2.ReadSession do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:schema, 0)

  field :name, 1, type: :string, deprecated: false

  field :expire_time, 2,
    type: Google.Protobuf.Timestamp,
    json_name: "expireTime",
    deprecated: false

  field :data_format, 3,
    type: Google.Cloud.Bigquery.Storage.V1beta2.DataFormat,
    json_name: "dataFormat",
    enum: true,
    deprecated: false

  field :avro_schema, 4,
    type: Google.Cloud.Bigquery.Storage.V1beta2.AvroSchema,
    json_name: "avroSchema",
    oneof: 0,
    deprecated: false

  field :arrow_schema, 5,
    type: Google.Cloud.Bigquery.Storage.V1beta2.ArrowSchema,
    json_name: "arrowSchema",
    oneof: 0,
    deprecated: false

  field :table, 6, type: :string, deprecated: false

  field :table_modifiers, 7,
    type: Google.Cloud.Bigquery.Storage.V1beta2.ReadSession.TableModifiers,
    json_name: "tableModifiers",
    deprecated: false

  field :read_options, 8,
    type: Google.Cloud.Bigquery.Storage.V1beta2.ReadSession.TableReadOptions,
    json_name: "readOptions",
    deprecated: false

  field :streams, 10,
    repeated: true,
    type: Google.Cloud.Bigquery.Storage.V1beta2.ReadStream,
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1beta2.ReadStream do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1beta2.WriteStream do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false

  field :type, 2,
    type: Google.Cloud.Bigquery.Storage.V1beta2.WriteStream.Type,
    enum: true,
    deprecated: false

  field :create_time, 3,
    type: Google.Protobuf.Timestamp,
    json_name: "createTime",
    deprecated: false

  field :commit_time, 4,
    type: Google.Protobuf.Timestamp,
    json_name: "commitTime",
    deprecated: false

  field :table_schema, 5,
    type: Google.Cloud.Bigquery.Storage.V1beta2.TableSchema,
    json_name: "tableSchema",
    deprecated: false
end
