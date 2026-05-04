alias Logflare.LogEvent
alias Logflare.Sources.Source

import Logflare.Factory

# Setup test data
user = insert(:user)
source = insert(:source, user: user)

# Insert KV pairs for lookup
for i <- 1..100 do
  insert(:key_value,
    user: user,
    key: "key_#{i}",
    value: %{"org_id" => "org_#{i}", "name" => "Name #{i}", "nested" => %{"id" => i}}
  )
end

# Source with no kv enrichment
source_no_kv = %{source | transform_key_values: nil, transform_key_values_parsed: nil}

# Source with 1 rule (dot accessor), pre-parsed
source_1_rule_dot =
  %{source | transform_key_values: "project:org_id:org_id"}
  |> Source.parse_key_values_config()

# Source with 1 rule (jsonpath accessor), pre-parsed
source_1_rule_jsonpath =
  %{source | transform_key_values: "project:nested_id:$.nested.id"}
  |> Source.parse_key_values_config()

# Event params
simple_event = %{"project" => "key_1", "event_message" => "test"}

Benchee.run(
  %{
    "no kv enrichment (nil config)" => fn ->
      LogEvent.make(simple_event, %{source: source_no_kv})
    end,
    "1 rule dot accessor (pre-parsed)" => fn ->
      LogEvent.make(simple_event, %{source: source_1_rule_dot})
    end,
    "1 rule jsonpath accessor (pre-parsed)" => fn ->
      LogEvent.make(simple_event, %{source: source_1_rule_jsonpath})
    end
  },
  time: 4,
  warmup: 1,
  memory_time: 3,
  reduction_time: 3
)
