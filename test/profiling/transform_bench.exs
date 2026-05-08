# Usage: MIX_ENV=test mix run test/profiling/transform_bench.exs
#
# Measures LogEvent.make/2 throughput on the ingest hot path across transform
# configurations. Compares the pre-parsed (cached) virtual against the
# unparsed-fallback path to surface the benefit of the parsed-virtual caching.
#
# Run under MIX_ENV=test so the "key_values" feature flag returns true
# unconditionally (see Logflare.Utils.flag/2) and the dev-env Phoenix asset
# watcher doesn't contaminate the first scenario.

import Logflare.Factory

alias Logflare.LogEvent
alias Logflare.Sources.Source

user = insert(:user)
source = insert(:source, user: user)

# KV lookup targets used by the kv_enrich scenarios. Match the values that
# appear under the "service" and "namespace" keys in the bench payload below
# so each kv rule actually hits.
insert(:key_value,
  user: user,
  key: "router",
  value: %{"org_id" => "acme", "name" => "Acme"}
)

insert(:key_value,
  user: user,
  key: "default",
  value: %{"org_id" => "default-org", "name" => "Default"}
)

payload = fn ->
  %{
    "event_message" => "bench",
    "service" => "router",
    "namespace" => "default",
    "extra-key" => "needs-sanitizing",
    "metadata" => %{
      "user" => %{"id" => 42, "name" => "ada"},
      "routing" => %{"region" => "us-east", "zone" => "a"}
    }
  }
end

copy_config = """
service:metadata.routing.service
namespace:metadata.routing.namespace
metadata.user.id:metadata.flat.user_id
metadata.user.name:metadata.flat.user_name
m.routing.zone:metadata.flat.zone
"""

kv_config = """
service:enriched_service:org_id
namespace:enriched_ns:org_id
"""

drop_config = """
service
namespace
metadata.user.id
metadata.routing.region
m.routing.zone
"""

with_copy_parsed =
  %{source | transform_copy_fields: copy_config}
  |> Source.parse_copy_fields_config()

with_copy_unparsed = %{source | transform_copy_fields: copy_config}

with_kv_parsed =
  %{source | transform_key_values: kv_config}
  |> Source.parse_key_values_config()

with_kv_unparsed = %{source | transform_key_values: kv_config}

with_drop_parsed =
  %{source | transform_drop_fields: drop_config}
  |> Source.parse_drop_fields_config()

with_drop_unparsed = %{source | transform_drop_fields: drop_config}

with_all_parsed =
  %{
    source
    | transform_copy_fields: copy_config,
      transform_key_values: kv_config,
      transform_drop_fields: drop_config
  }
  |> Source.parse_copy_fields_config()
  |> Source.parse_key_values_config()
  |> Source.parse_drop_fields_config()

Benchee.run(
  %{
    "baseline (no transforms)" => fn ->
      LogEvent.make(payload.(), %{source: source})
    end,
    "copy_fields (parsed)" => fn ->
      LogEvent.make(payload.(), %{source: with_copy_parsed})
    end,
    "copy_fields (unparsed fallback)" => fn ->
      LogEvent.make(payload.(), %{source: with_copy_unparsed})
    end,
    "kv_enrich (parsed)" => fn ->
      LogEvent.make(payload.(), %{source: with_kv_parsed})
    end,
    "kv_enrich (unparsed fallback)" => fn ->
      LogEvent.make(payload.(), %{source: with_kv_unparsed})
    end,
    "drop_fields (parsed)" => fn ->
      LogEvent.make(payload.(), %{source: with_drop_parsed})
    end,
    "drop_fields (unparsed fallback)" => fn ->
      LogEvent.make(payload.(), %{source: with_drop_unparsed})
    end,
    "copy + kv + drop (parsed)" => fn ->
      LogEvent.make(payload.(), %{source: with_all_parsed})
    end
  },
  time: 10,
  warmup: 2,
  memory_time: 1
)
