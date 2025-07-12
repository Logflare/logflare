defmodule Google.Cloud.Bigquery.V2.PrimaryKey do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :columns, 1, repeated: true, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.ColumnReference do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :referencing_column, 1, type: :string, json_name: "referencingColumn", deprecated: false
  field :referenced_column, 2, type: :string, json_name: "referencedColumn", deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.ForeignKey do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false

  field :referenced_table, 2,
    type: Google.Cloud.Bigquery.V2.TableReference,
    json_name: "referencedTable",
    deprecated: false

  field :column_references, 3,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.ColumnReference,
    json_name: "columnReferences",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.TableConstraints do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :primary_key, 1,
    type: Google.Cloud.Bigquery.V2.PrimaryKey,
    json_name: "primaryKey",
    deprecated: false

  field :foreign_keys, 2,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.ForeignKey,
    json_name: "foreignKeys",
    deprecated: false
end
