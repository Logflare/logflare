defmodule Google.Cloud.Bigquery.V2.JoinRestrictionPolicy.JoinCondition do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :JOIN_CONDITION_UNSPECIFIED, 0
  field :JOIN_ANY, 1
  field :JOIN_ALL, 2
  field :JOIN_NOT_REQUIRED, 3
  field :JOIN_BLOCKED, 4
end

defmodule Google.Cloud.Bigquery.V2.AggregationThresholdPolicy do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :threshold, 1, proto3_optional: true, type: :int64, deprecated: false

  field :privacy_unit_columns, 2,
    repeated: true,
    type: :string,
    json_name: "privacyUnitColumns",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.DifferentialPrivacyPolicy do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :max_epsilon_per_query, 1,
    proto3_optional: true,
    type: :double,
    json_name: "maxEpsilonPerQuery",
    deprecated: false

  field :delta_per_query, 2,
    proto3_optional: true,
    type: :double,
    json_name: "deltaPerQuery",
    deprecated: false

  field :max_groups_contributed, 3,
    proto3_optional: true,
    type: :int64,
    json_name: "maxGroupsContributed",
    deprecated: false

  field :privacy_unit_column, 4,
    proto3_optional: true,
    type: :string,
    json_name: "privacyUnitColumn",
    deprecated: false

  field :epsilon_budget, 5,
    proto3_optional: true,
    type: :double,
    json_name: "epsilonBudget",
    deprecated: false

  field :delta_budget, 6,
    proto3_optional: true,
    type: :double,
    json_name: "deltaBudget",
    deprecated: false

  field :epsilon_budget_remaining, 7,
    proto3_optional: true,
    type: :double,
    json_name: "epsilonBudgetRemaining",
    deprecated: false

  field :delta_budget_remaining, 8,
    proto3_optional: true,
    type: :double,
    json_name: "deltaBudgetRemaining",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.JoinRestrictionPolicy do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :join_condition, 1,
    proto3_optional: true,
    type: Google.Cloud.Bigquery.V2.JoinRestrictionPolicy.JoinCondition,
    json_name: "joinCondition",
    enum: true,
    deprecated: false

  field :join_allowed_columns, 2,
    repeated: true,
    type: :string,
    json_name: "joinAllowedColumns",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.PrivacyPolicy do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:privacy_policy, 0)

  field :aggregation_threshold_policy, 2,
    type: Google.Cloud.Bigquery.V2.AggregationThresholdPolicy,
    json_name: "aggregationThresholdPolicy",
    oneof: 0,
    deprecated: false

  field :differential_privacy_policy, 3,
    type: Google.Cloud.Bigquery.V2.DifferentialPrivacyPolicy,
    json_name: "differentialPrivacyPolicy",
    oneof: 0,
    deprecated: false

  field :join_restriction_policy, 1,
    proto3_optional: true,
    type: Google.Cloud.Bigquery.V2.JoinRestrictionPolicy,
    json_name: "joinRestrictionPolicy",
    deprecated: false
end
