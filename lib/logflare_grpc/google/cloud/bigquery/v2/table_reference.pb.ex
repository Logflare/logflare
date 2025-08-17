defmodule Google.Cloud.Bigquery.V2.TableReference do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId", deprecated: false
  field :dataset_id, 2, type: :string, json_name: "datasetId", deprecated: false
  field :table_id, 3, type: :string, json_name: "tableId", deprecated: false
end
