defmodule Google.Cloud.Bigquery.V2.StandardSqlDataType.TypeKind do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :TYPE_KIND_UNSPECIFIED, 0
  field :INT64, 2
  field :BOOL, 5
  field :FLOAT64, 7
  field :STRING, 8
  field :BYTES, 9
  field :TIMESTAMP, 19
  field :DATE, 10
  field :TIME, 20
  field :DATETIME, 21
  field :INTERVAL, 26
  field :GEOGRAPHY, 22
  field :NUMERIC, 23
  field :BIGNUMERIC, 24
  field :JSON, 25
  field :ARRAY, 16
  field :STRUCT, 17
  field :RANGE, 29
end

defmodule Google.Cloud.Bigquery.V2.StandardSqlDataType do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:sub_type, 0)

  field :type_kind, 1,
    type: Google.Cloud.Bigquery.V2.StandardSqlDataType.TypeKind,
    json_name: "typeKind",
    enum: true,
    deprecated: false

  field :array_element_type, 2,
    type: Google.Cloud.Bigquery.V2.StandardSqlDataType,
    json_name: "arrayElementType",
    oneof: 0

  field :struct_type, 3,
    type: Google.Cloud.Bigquery.V2.StandardSqlStructType,
    json_name: "structType",
    oneof: 0

  field :range_element_type, 4,
    type: Google.Cloud.Bigquery.V2.StandardSqlDataType,
    json_name: "rangeElementType",
    oneof: 0
end

defmodule Google.Cloud.Bigquery.V2.StandardSqlField do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
  field :type, 2, type: Google.Cloud.Bigquery.V2.StandardSqlDataType, deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.StandardSqlStructType do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :fields, 1, repeated: true, type: Google.Cloud.Bigquery.V2.StandardSqlField
end

defmodule Google.Cloud.Bigquery.V2.StandardSqlTableType do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :columns, 1, repeated: true, type: Google.Cloud.Bigquery.V2.StandardSqlField
end
