# credo:disable-for-this-file Credo.Check.Refactor.IoPuts
#
# Usage: MIX_ENV=test mix run test/profiling/log_event_make_profile.exs
#
# Surfaces per-function time inside LogEvent.make/2 via :tprof, using the
# realistic edge_log and otel_trace fixtures from log_event_make_bench.exs.
# The edge_log payload (~80 leaves of mixed-case Cloudflare metadata across
# 4-5 nesting levels) is the larger stress case and the default.
#
# Env:
#   ITERATIONS — iterations per profile run (default 2000)
#   TPROF_TYPE — time | calls | memory (default time)
#   FIXTURE    — edge_log | otel_trace (default edge_log)

alias Logflare.LogEvent

import Logflare.Factory

user = insert(:user)
source = insert(:source, user: user)

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

fixtures = %{"otel_trace" => otel_trace_params, "edge_log" => edge_log_params}
fixture_name = System.get_env("FIXTURE") || "edge_log"
params = Map.fetch!(fixtures, fixture_name)

iterations = String.to_integer(System.get_env("ITERATIONS") || "2000")
type = String.to_atom(System.get_env("TPROF_TYPE") || "time")

for _ <- 1..500, do: LogEvent.make(params, %{source: source})

IO.puts(
  "\n== :tprof #{type} over #{iterations} iterations of LogEvent.make/2 (#{fixture_name}) ==\n"
)

Mix.Tasks.Profile.Tprof.profile(
  fn ->
    for _ <- 1..iterations, do: LogEvent.make(params, %{source: source})
  end,
  type: type,
  warmup: 0,
  sort: :per_call
)
