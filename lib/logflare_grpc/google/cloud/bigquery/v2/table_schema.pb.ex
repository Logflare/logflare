defmodule Google.Cloud.Bigquery.V2.ForeignTypeInfo.TypeSystem do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :TYPE_SYSTEM_UNSPECIFIED, 0
  field :HIVE, 1
end

defmodule Google.Cloud.Bigquery.V2.TableFieldSchema.RoundingMode do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :ROUNDING_MODE_UNSPECIFIED, 0
  field :ROUND_HALF_AWAY_FROM_ZERO, 1
  field :ROUND_HALF_EVEN, 2
end

defmodule Google.Cloud.Bigquery.V2.TableSchema do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :fields, 1, repeated: true, type: Google.Cloud.Bigquery.V2.TableFieldSchema

  field :foreign_type_info, 3,
    type: Google.Cloud.Bigquery.V2.ForeignTypeInfo,
    json_name: "foreignTypeInfo",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.ForeignTypeInfo do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :type_system, 1,
    type: Google.Cloud.Bigquery.V2.ForeignTypeInfo.TypeSystem,
    json_name: "typeSystem",
    enum: true,
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.DataPolicyOption do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, proto3_optional: true, type: :string
end

defmodule Google.Cloud.Bigquery.V2.TableFieldSchema.PolicyTagList do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :names, 1, repeated: true, type: :string
end

defmodule Google.Cloud.Bigquery.V2.TableFieldSchema.FieldElementType do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :type, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.TableFieldSchema do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
  field :type, 2, type: :string, deprecated: false
  field :mode, 3, type: :string, deprecated: false

  field :fields, 4,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.TableFieldSchema,
    deprecated: false

  field :description, 6, type: Google.Protobuf.StringValue, deprecated: false

  field :policy_tags, 9,
    type: Google.Cloud.Bigquery.V2.TableFieldSchema.PolicyTagList,
    json_name: "policyTags",
    deprecated: false

  field :data_policies, 21,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.DataPolicyOption,
    json_name: "dataPolicies",
    deprecated: false

  field :max_length, 10, type: :int64, json_name: "maxLength", deprecated: false
  field :precision, 11, type: :int64, deprecated: false
  field :scale, 12, type: :int64, deprecated: false

  field :rounding_mode, 15,
    type: Google.Cloud.Bigquery.V2.TableFieldSchema.RoundingMode,
    json_name: "roundingMode",
    enum: true,
    deprecated: false

  field :collation, 13, type: Google.Protobuf.StringValue, deprecated: false

  field :default_value_expression, 14,
    type: Google.Protobuf.StringValue,
    json_name: "defaultValueExpression",
    deprecated: false

  field :range_element_type, 18,
    type: Google.Cloud.Bigquery.V2.TableFieldSchema.FieldElementType,
    json_name: "rangeElementType",
    deprecated: false

  field :foreign_type_definition, 23,
    type: :string,
    json_name: "foreignTypeDefinition",
    deprecated: false
end
