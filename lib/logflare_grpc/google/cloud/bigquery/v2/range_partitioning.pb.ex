defmodule Google.Cloud.Bigquery.V2.RangePartitioning.Range do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :start, 1, type: :string, deprecated: false
  field :end, 2, type: :string, deprecated: false
  field :interval, 3, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.RangePartitioning do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :field, 1, type: :string, deprecated: false
  field :range, 2, type: Google.Cloud.Bigquery.V2.RangePartitioning.Range
end
