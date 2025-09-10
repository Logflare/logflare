defmodule Google.Cloud.Bigquery.Storage.V1.TableFieldSchema.Type do
  @moduledoc false

  use Protobuf, enum: true, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :TYPE_UNSPECIFIED, 0
  field :STRING, 1
  field :INT64, 2
  field :DOUBLE, 3
  field :STRUCT, 4
  field :BYTES, 5
  field :BOOL, 6
  field :TIMESTAMP, 7
  field :DATE, 8
  field :TIME, 9
  field :DATETIME, 10
  field :GEOGRAPHY, 11
  field :NUMERIC, 12
  field :BIGNUMERIC, 13
  field :INTERVAL, 14
  field :JSON, 15
  field :RANGE, 16
end

defmodule Google.Cloud.Bigquery.Storage.V1.TableFieldSchema.Mode do
  @moduledoc false

  use Protobuf, enum: true, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :MODE_UNSPECIFIED, 0
  field :NULLABLE, 1
  field :REQUIRED, 2
  field :REPEATED, 3
end

defmodule Google.Cloud.Bigquery.Storage.V1.TableSchema do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :fields, 1, repeated: true, type: Google.Cloud.Bigquery.Storage.V1.TableFieldSchema
end

defmodule Google.Cloud.Bigquery.Storage.V1.TableFieldSchema.FieldElementType do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :type, 1,
    type: Google.Cloud.Bigquery.Storage.V1.TableFieldSchema.Type,
    enum: true,
    deprecated: false
end

defmodule Google.Cloud.Bigquery.Storage.V1.TableFieldSchema do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :name, 1, type: :string, deprecated: false

  field :type, 2,
    type: Google.Cloud.Bigquery.Storage.V1.TableFieldSchema.Type,
    enum: true,
    deprecated: false

  field :mode, 3,
    type: Google.Cloud.Bigquery.Storage.V1.TableFieldSchema.Mode,
    enum: true,
    deprecated: false

  field :fields, 4,
    repeated: true,
    type: Google.Cloud.Bigquery.Storage.V1.TableFieldSchema,
    deprecated: false

  field :description, 6, type: :string, deprecated: false
  field :max_length, 7, type: :int64, json_name: "maxLength", deprecated: false
  field :precision, 8, type: :int64, deprecated: false
  field :scale, 9, type: :int64, deprecated: false

  field :default_value_expression, 10,
    type: :string,
    json_name: "defaultValueExpression",
    deprecated: false

  field :range_element_type, 11,
    type: Google.Cloud.Bigquery.Storage.V1.TableFieldSchema.FieldElementType,
    json_name: "rangeElementType",
    deprecated: false
end
