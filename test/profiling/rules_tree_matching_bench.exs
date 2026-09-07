# credo:disable-for-this-file Credo.Check.Refactor.IoPuts
#
# Usage: MIX_ENV=test mix run test/profiling/rules_tree_matching_bench.exs
#
# Env:
#   SAVE_SNAPSHOT=1     — append this run's results to rules_tree_matching_bench.history.exs
#   LABEL="..."         — optional label for the new entry (e.g., "post fast path")
#   MACHINE="..."       — optional machine identifier so cross-machine entries can be filtered
#   PROFILE=1           — run :tprof against each scenario after the benchmark (Benchee profile_after)
#   TPROF_TYPE=time     — :tprof type when PROFILE=1 (time | calls | memory; default time)
#
# Every run prints a delta table vs the most-recent entry in the history file
# (if one exists). The history file is the source of truth for trend tracking.

alias Logflare.LogEvent
alias Logflare.Lql.Parser
alias Logflare.Lql.Rules.FilterRule
alias Logflare.Profiling
alias Logflare.Rules.Rule
alias Logflare.Sources.Cache
alias Logflare.Sources.SourceRouter.RulesTree

import Logflare.Factory

# Sources.Cache.get_by_and_preload_rules/1 transitively hits Billing.get_plan_by/1
# (via Sources.put_retention_days/1), which uses Repo.get_by and raises if there's
# more than one Free plan. mix run isn't sandboxed like ExUnit, so insert only when
# no Free plan exists yet — that keeps repeated bench runs from accumulating
# duplicate plans without disturbing whatever local DB state a dev has set up.
Logflare.Repo.get_by(Logflare.Billing.Plan, name: "Free") || insert(:plan)

user = insert(:user)

source_record = insert(:source, user: user)
source = Cache.get_by_and_preload_rules(token: source_record.token)

# ---------------------------------------------------------------------------
# Payloads — kept identical to log_event_make_bench.exs so that the
# flattened_body produced here matches what production ingestion produces.
# ---------------------------------------------------------------------------

# OTEL trace payload (~15 leaf values, 2-3 levels deep)
otel_trace_params = %{
  "attributes" => %{
    "_client_address" => "2600:1fc4:22a5:ae73:2887:684b:66c0:1f85",
    "_http_request_method" => "POST",
    "_http_route" => "/functions/v1/*path",
    "_http_status_code" => 404,
    "_network_peer_address" => "99.88.160.11",
    "_network_peer_port" => 57_785,
    "_network_protocol_version" => "1.1",
    "_server_address" => "supabase-api-gateway",
    "_url_path" => "/functions/v1/stripe-worker",
    "_url_scheme" => "https",
    "_user_agent_original" => "pg_net/0.19.5"
  },
  "end_time" => "2026-01-21T17:54:48.444334Z",
  "event_message" => "POST /functions/v1/*path",
  "metadata" => %{"type" => "span"},
  "project" => "supabase-api-gateway",
  "resource" => %{
    "cloud" => %{"region" => "us-east-1"},
    "deployment" => %{"environment" => "staging"},
    "service" => %{"name" => "supabase-api-gateway", "version" => "1.0.0"}
  },
  "scope" => %{
    "name" => "go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin",
    "version" => "0.61.0"
  },
  "span_id" => "c99f33f1bfa4fb8f",
  "start_time" => "2026-01-21T17:54:48.144506Z",
  "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
  "trace_id" => "f9918d38f5d1cb74ec5656b9a315e5f6"
}

# Edge log payload (~80 leaf values, 4-5 levels deep).
#
# NOTE: the `cf` subtree (botManagement + ja4Signals + tlsClientAuth) overstates
# the current production shape — those subtrees have been trimmed upstream. We
# keep the original shape here so existing history entries remain comparable.
# When refreshing baselines, trim cf to match prod and reseed the history file.
edge_log_params = %{
  "event_message" =>
    "POST | 200 | 3.254.227.51 | 9ca90d4f3f7cc99e | https://example.supabase.co/rest/v1/calls",
  "id" => "3701391c-dead-405d-9943-61cc7576da87",
  "identifier" => "example_project",
  "metadata" => %{
    "logflare_worker" => %{"worker_id" => "ZZ79SS"},
    "request" => %{
      "cf" => %{
        "asOrganization" => "Amazon Data Services Ireland Limited",
        "asn" => 16_509,
        "botManagement" => %{
          "corporateProxy" => false,
          "ja3Hash" => "a1465cad7ff59adb0e36ccd6339ba5db",
          "ja4" => "t23e1011h2_61a7dd8aa9b6_3fcd1a44f3e3",
          "ja4Signals" => %{
            "browser_ratio_1h" => 0.0026,
            "cache_ratio_1h" => 0.0389,
            "h2h3_ratio_1h" => 0.9899,
            "heuristic_ratio_1h" => 0.2229,
            "ips_quantile_1h" => 0.9997,
            "ips_rank_1h" => 218,
            "paths_rank_1h" => 16,
            "reqs_quantile_1h" => 0.9999,
            "reqs_rank_1h" => 36,
            "uas_rank_1h" => 294
          },
          "jsDetection" => %{"passed" => false},
          "score" => 46,
          "staticResource" => false,
          "verifiedBot" => false
        },
        "city" => "Dublin",
        "clientAcceptEncoding" => "gzip, br",
        "clientTcpRtt" => 4,
        "clientTrustScore" => 46,
        "colo" => "DUB",
        "continent" => "EU",
        "country" => "IE",
        "edgeRequestKeepAliveStatus" => 1,
        "httpProtocol" => "HTTP/2",
        "latitude" => "53.33326",
        "longitude" => "-6.24789",
        "postalCode" => "D02",
        "region" => "Leinster",
        "regionCode" => "L",
        "requestPriority" => "weight=16;exclusive=0",
        "timezone" => "Europe/Dublin",
        "tlsCipher" => "AEAD-AES256-GCM-SHA384",
        "tlsClientAuth" => %{
          "certPresented" => "0",
          "certRevoked" => "0",
          "certVerified" => "NONE"
        },
        "tlsVersion" => "TLSv1.3"
      },
      "headers" => %{
        "accept" => "*/*",
        "cf_connecting_ip" => "3.254.227.51",
        "cf_ipcountry" => "IE",
        "cf_ray" => "9ca90d4f3f7cc99e",
        "content_length" => "18466",
        "content_type" => "application/json",
        "host" => "example.supabase.co",
        "user_agent" => "Deno/2.1.4"
      },
      "host" => "example.supabase.co",
      "method" => "POST",
      "path" => "/rest/v1/calls",
      "protocol" => "https:",
      "search" => "?on_conflict=call_id",
      "url" => "https://example.supabase.co/rest/v1/calls?on_conflict=call_id"
    },
    "response" => %{
      "headers" => %{
        "cf_cache_status" => "DYNAMIC",
        "cf_ray" => "ba903a6f47bfc12f-DUB",
        "content_length" => "0",
        "date" => "Tue, 16 Dec 2025 18:26:18 GMT",
        "sb_gateway_version" => "1"
      },
      "origin_time" => 12,
      "status_code" => 200
    }
  },
  "project" => "example_project",
  "request_id" => "a99b2a69-dead-752e-78bb-14ca90530fe1",
  "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
}

# ---------------------------------------------------------------------------
# Build LogEvent instances once — flattened_body is populated identically
# to production, isolating the matching cost from LogEvent.make/1 cost.
# ---------------------------------------------------------------------------
otel_le = LogEvent.make(otel_trace_params, %{source: source})
edge_le = LogEvent.make(edge_log_params, %{source: source})

# ---------------------------------------------------------------------------
# Rule builders
# ---------------------------------------------------------------------------

# Builds a single in-memory Rule struct from an LQL string.
# Parser is called once per rule at setup time so we don't pay parse cost per
# iteration. Only id and lql_filters are consulted by RulesTree.build/1.
make_rule = fn id, lql_string ->
  {:ok, filters} = Parser.parse(lql_string)
  %Rule{id: id, lql_filters: filters}
end

# Builds a FilterRule directly without going through the LQL parser.
make_filter = fn path, operator, value ->
  %FilterRule{path: path, operator: operator, value: value, modifiers: %{}}
end

# ---------------------------------------------------------------------------
# Scenario 1: empty rule set
# ---------------------------------------------------------------------------
empty_tree_otel = RulesTree.build([])
empty_tree_edge = RulesTree.build([])

# ---------------------------------------------------------------------------
# Scenario 2: 100 single project= rules, event matches one
#
# OTEL event has project:"supabase-api-gateway". We reserve that value for
# rule 99 and use "v0".."v98" for the other 99 rules.
# Edge event has project:"example_project". We reserve that value for rule 99
# and use "v0".."v98" for the other 99 rules.
# ---------------------------------------------------------------------------
drain_100_match_otel_rules =
  for(i <- 0..98, do: make_rule.(i, "project:v#{i}")) ++
    [make_rule.(99, "project:supabase-api-gateway")]

drain_100_match_edge_rules =
  for(i <- 0..98, do: make_rule.(i, "project:v#{i}")) ++
    [make_rule.(99, "project:example_project")]

drain_100_match_otel_tree = RulesTree.build(drain_100_match_otel_rules)
drain_100_match_edge_tree = RulesTree.build(drain_100_match_edge_rules)

# ---------------------------------------------------------------------------
# Scenario 3: 100 single project= rules, event misses all
# ---------------------------------------------------------------------------
drain_100_miss_rules =
  for i <- 0..99, do: make_rule.(i, "project:miss_#{i}")

drain_100_miss_tree = RulesTree.build(drain_100_miss_rules)

# ---------------------------------------------------------------------------
# Scenario 4: 1000 single project= rules, event matches one
# ---------------------------------------------------------------------------
drain_1000_match_otel_rules =
  for(i <- 0..998, do: make_rule.(i, "project:v#{i}")) ++
    [make_rule.(999, "project:supabase-api-gateway")]

drain_1000_match_edge_rules =
  for(i <- 0..998, do: make_rule.(i, "project:v#{i}")) ++
    [make_rule.(999, "project:example_project")]

drain_1000_match_otel_tree = RulesTree.build(drain_1000_match_otel_rules)
drain_1000_match_edge_tree = RulesTree.build(drain_1000_match_edge_rules)

# ---------------------------------------------------------------------------
# Scenario 5: 100 scattered single-equality rules, each on a unique path
#
# Fields of the form "fieldN:valN" — neither OTEL nor edge events have these
# top-level fields, so every rule is a miss. Cost measured is the traversal.
# ---------------------------------------------------------------------------
scattered_100_rules =
  for i <- 0..99 do
    %Rule{id: i, lql_filters: [make_filter.("field#{i}", :=, "val#{i}")]}
  end

scattered_100_tree = RulesTree.build(scattered_100_rules)

# ---------------------------------------------------------------------------
# Scenario 6: mixed 100 rules — 50 single-equality + 50 non-equality
#
# Non-equality half: mix of regex, comparison (gt), multi-filter, negation.
# IDs 0..49: single equality on project:vN
# IDs 50..61: regex on event_message
# IDs 62..73: comparison (>=) on metadata.response.status
# IDs 74..85: multi-filter (project + level)
# IDs 86..99: negated equality on project
# ---------------------------------------------------------------------------
mixed_100_rules =
  for(i <- 0..49, do: make_rule.(i, "project:v#{i}")) ++
    for i <- 0..11 do
      make_rule.(50 + i, "event_message:~\"pattern#{i}\"")
    end ++
    for i <- 0..11 do
      %Rule{
        id: 62 + i,
        lql_filters: [make_filter.("metadata.response.status", :>=, 200 + i)]
      }
    end ++
    for i <- 0..11 do
      %Rule{
        id: 74 + i,
        lql_filters: [
          make_filter.("project", :=, "multi#{i}"),
          make_filter.("level", :=, "error")
        ]
      }
    end ++
    for i <- 0..13 do
      %Rule{
        id: 86 + i,
        lql_filters: [
          %FilterRule{path: "project", operator: :=, value: "neg#{i}", modifiers: %{negate: true}}
        ]
      }
    end

mixed_100_otel_tree = RulesTree.build(mixed_100_rules)
mixed_100_edge_tree = RulesTree.build(mixed_100_rules)

# ---------------------------------------------------------------------------
# Scenario 7: 100 regex-only rules — none qualify for the fast path
# ---------------------------------------------------------------------------
regex_100_rules =
  for i <- 0..99 do
    make_rule.(i, "event_message:~\"pattern#{i}\"")
  end

regex_100_tree = RulesTree.build(regex_100_rules)

# ---------------------------------------------------------------------------
# Scenario 8: 100 comparison rules on an existing numeric field, all match
# ---------------------------------------------------------------------------
build_comparison_rules = fn path, base ->
  for i <- 0..99 do
    {op, value} =
      case rem(i, 4) do
        0 -> {:>, base - i - 1}
        1 -> {:>=, base - i}
        2 -> {:<, base + i + 1}
        3 -> {:<=, base + i}
      end

    %Rule{id: i, lql_filters: [make_filter.(path, op, value)]}
  end
end

comparison_100_otel_rules = build_comparison_rules.("attributes._http_status_code", 404)
comparison_100_edge_rules = build_comparison_rules.("metadata.response.status_code", 200)

comparison_100_otel_tree = RulesTree.build(comparison_100_otel_rules)
comparison_100_edge_tree = RulesTree.build(comparison_100_edge_rules)

# ---------------------------------------------------------------------------
# Profile setup
# ---------------------------------------------------------------------------
profile_after =
  if System.get_env("PROFILE") == "1" do
    type = String.to_existing_atom(System.get_env("TPROF_TYPE") || "time")
    {:tprof, type: type, warmup: 0, sort: :per_call}
  else
    false
  end

# ---------------------------------------------------------------------------
# Benchee
# ---------------------------------------------------------------------------
suite =
  Benchee.run(
    %{
      # Scenario 1: empty
      "rt empty | otel" => fn -> RulesTree.matching_rule_ids(otel_le, empty_tree_otel) end,
      "rt empty | edge" => fn -> RulesTree.matching_rule_ids(edge_le, empty_tree_edge) end,
      # Scenario 2: 100 project= match
      "rt drain 100 project= match | otel" => fn ->
        RulesTree.matching_rule_ids(otel_le, drain_100_match_otel_tree)
      end,
      "rt drain 100 project= match | edge" => fn ->
        RulesTree.matching_rule_ids(edge_le, drain_100_match_edge_tree)
      end,
      # Scenario 3: 100 project= miss
      "rt drain 100 project= miss | otel" => fn ->
        RulesTree.matching_rule_ids(otel_le, drain_100_miss_tree)
      end,
      "rt drain 100 project= miss | edge" => fn ->
        RulesTree.matching_rule_ids(edge_le, drain_100_miss_tree)
      end,
      # Scenario 4: 1000 project= match
      "rt drain 1000 project= match | otel" => fn ->
        RulesTree.matching_rule_ids(otel_le, drain_1000_match_otel_tree)
      end,
      "rt drain 1000 project= match | edge" => fn ->
        RulesTree.matching_rule_ids(edge_le, drain_1000_match_edge_tree)
      end,
      # Scenario 5: 100 scattered unique paths, miss
      "rt scattered 100 path= match | otel" => fn ->
        RulesTree.matching_rule_ids(otel_le, scattered_100_tree)
      end,
      "rt scattered 100 path= match | edge" => fn ->
        RulesTree.matching_rule_ids(edge_le, scattered_100_tree)
      end,
      # Scenario 6: mixed 100
      "rt mixed 100 | otel" => fn ->
        RulesTree.matching_rule_ids(otel_le, mixed_100_otel_tree)
      end,
      "rt mixed 100 | edge" => fn ->
        RulesTree.matching_rule_ids(edge_le, mixed_100_edge_tree)
      end,
      # Scenario 7: regex-only 100
      "rt regex only 100 | otel" => fn ->
        RulesTree.matching_rule_ids(otel_le, regex_100_tree)
      end,
      "rt regex only 100 | edge" => fn ->
        RulesTree.matching_rule_ids(edge_le, regex_100_tree)
      end,
      # Scenario 8: comparison-only 100, all match
      "rt comparison 100 | otel" => fn ->
        RulesTree.matching_rule_ids(otel_le, comparison_100_otel_tree)
      end,
      "rt comparison 100 | edge" => fn ->
        RulesTree.matching_rule_ids(edge_le, comparison_100_edge_tree)
      end
    },
    time: 5,
    warmup: 2,
    memory_time: 3,
    reduction_time: 3,
    profile_after: profile_after
  )

history_path = Path.expand("rules_tree_matching_bench.history.exs", __DIR__)
Profiling.track(suite, history_path)
