defmodule Google.Cloud.Bigquery.V2.RestrictionConfig.RestrictionType do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :RESTRICTION_TYPE_UNSPECIFIED, 0
  field :RESTRICTED_DATA_EGRESS, 1
end

defmodule Google.Cloud.Bigquery.V2.RestrictionConfig do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :type, 1,
    type: Google.Cloud.Bigquery.V2.RestrictionConfig.RestrictionType,
    enum: true,
    deprecated: false
end
