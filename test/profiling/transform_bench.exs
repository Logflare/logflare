# Usage: MIX_ENV=test mix run bench/transform_bench.exs
#
# Measures LogEvent.make/2 throughput on the ingest hot path across transform
# configurations. Compares the pre-parsed (cached) virtual against the
# unparsed-fallback path to surface the benefit of the parsed-virtual caching.
#
# Run under MIX_ENV=test to avoid the dev-env Phoenix asset watcher
# contaminating the first scenario.
#
# kv_enrich's KeyValues.Cache lookup and the "key_values" feature flag are
# stubbed via Mimic to a constant hot-cache hit. This isolates the transform
# pipeline cost from cache/DB cost — the kv_enrich numbers do NOT reflect
# production latency under cache misses or backing-store load.

alias Logflare.LogEvent
alias Logflare.Sources.Source

Mimic.copy(Logflare.KeyValues.Cache)
Mimic.copy(Logflare.Utils)
Mimic.set_mimic_global()

# Pre-build the cache return so the stub doesn't allocate a fresh map per
# call — Cachex returns a stored term in production, so this matches reality
# better. (The remaining kv_enrich variance is dominated by Mimic stub
# dispatch overhead, not allocation, and isn't addressable without replacing
# the stubbing strategy.)
kv_value = %{"org_id" => "acme", "name" => "Acme"}
Mimic.stub(Logflare.KeyValues.Cache, :lookup, fn _user_id, _key, _accessor_path ->
  kv_value
end)

Mimic.stub(Logflare.Utils, :flag, fn _feature, _identifier -> true end)

source = %Source{
  id: 1,
  user_id: 1,
  token: Ecto.UUID.generate(),
  name: "bench-source"
}

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
    "kv_enrich (parsed, stubbed hit)" => fn ->
      LogEvent.make(payload.(), %{source: with_kv_parsed})
    end,
    "kv_enrich (unparsed fallback, stubbed hit)" => fn ->
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
