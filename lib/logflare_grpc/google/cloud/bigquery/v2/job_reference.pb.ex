defmodule Google.Cloud.Bigquery.V2.JobReference do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId", deprecated: false
  field :job_id, 2, type: :string, json_name: "jobId", deprecated: false
  field :location, 3, type: Google.Protobuf.StringValue, deprecated: false
end
