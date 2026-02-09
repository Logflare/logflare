alias Logflare.LogEvent
alias Logflare.KeyValues
alias Logflare.Sources.Source

import Logflare.Factory

# Setup test data
user = insert(:user)
source = insert(:source, user: user)

# Insert KV pairs for lookup
for i <- 1..100 do
  insert(:key_value, user: user, key: "key_#{i}", value: "value_#{i}")
end

# Source with no kv enrichment
source_no_kv = %{source | transform_key_values: nil, transform_key_values_parsed: nil}

# Source with 1 rule, pre-parsed
source_1_rule =
  %{source | transform_key_values: "project:org_id"}
  |> Source.parse_key_values_config()

# Source with 5 rules, pre-parsed
source_5_rules =
  %{
    source
    | transform_key_values: Enum.map_join(1..5, "\n", fn i -> "field_#{i}:enriched_#{i}" end)
  }
  |> Source.parse_key_values_config()

# Source with 1 rule, NOT pre-parsed (fallback path)
source_1_rule_unparsed = %{
  source
  | transform_key_values: "project:org_id",
    transform_key_values_parsed: nil
}

# Event params
simple_event = %{"project" => "key_1", "event_message" => "test"}

multi_event =
  Map.merge(
    %{"event_message" => "test"},
    Map.new(1..5, fn i -> {"field_#{i}", "key_#{i}"} end)
  )

Benchee.run(
  %{
    "no kv enrichment (nil config)" => fn ->
      LogEvent.make(simple_event, %{source: source_no_kv})
    end,
    "1 rule (pre-parsed)" => fn ->
      LogEvent.make(simple_event, %{source: source_1_rule})
    end,
    "1 rule (unparsed fallback)" => fn ->
      LogEvent.make(simple_event, %{source: source_1_rule_unparsed})
    end,
    "5 rules (pre-parsed)" => fn ->
      LogEvent.make(multi_event, %{source: source_5_rules})
    end
  },
  time: 4,
  warmup: 1,
  memory_time: 3,
  reduction_time: 3
)
