# Usage: MIX_ENV=test mix run bench/transform_bench.exs
#
# Measures LogEvent.make/2 throughput on the ingest hot path across transform
# configurations. Compares the pre-parsed (cached) virtual against the
# unparsed-fallback path to surface the benefit of the parsed-virtual caching.
#
# Run under MIX_ENV=test to avoid the dev-env Phoenix asset watcher
# contaminating the first scenario.
#
# kv_enrich is intentionally excluded — it depends on KeyValues.Cache lookups
# and a feature flag, both of which would need stubbing for a clean comparison.

alias Logflare.LogEvent
alias Logflare.Sources.Source

source = %Source{
  id: 1,
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

with_drop_parsed =
  %{source | transform_drop_fields: drop_config}
  |> Source.parse_drop_fields_config()

with_drop_unparsed = %{source | transform_drop_fields: drop_config}

with_both_parsed =
  %{source | transform_copy_fields: copy_config, transform_drop_fields: drop_config}
  |> Source.parse_copy_fields_config()
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
    "drop_fields (parsed)" => fn ->
      LogEvent.make(payload.(), %{source: with_drop_parsed})
    end,
    "drop_fields (unparsed fallback)" => fn ->
      LogEvent.make(payload.(), %{source: with_drop_unparsed})
    end,
    "copy + drop (parsed)" => fn ->
      LogEvent.make(payload.(), %{source: with_both_parsed})
    end
  },
  time: 3,
  warmup: 2
)
