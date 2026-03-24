alias Logflare.LogEvent

import Logflare.Factory

# Setup test data
user = insert(:user)
source = insert(:source, user: user)

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

# Edge log payload (~80 leaf values, 4-5 levels deep)
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

Benchee.run(
  %{
    "otel trace" => fn ->
      LogEvent.make(otel_trace_params, %{source: source})
    end,
    "edge log" => fn ->
      LogEvent.make(edge_log_params, %{source: source})
    end
  },
  time: 5,
  warmup: 2,
  memory_time: 3,
  reduction_time: 3
)

# Results without flattening logic (baseline)
#
# Name                 ips        average  deviation         median         99th %
# otel trace       10.37 K       96.45 μs     ±7.56%       94.75 μs      119.88 μs
# edge log          2.69 K      371.63 μs     ±8.87%      362.90 μs      471.92 μs
#
# Memory usage statistics:
#
# Name               average  deviation         median         99th %
# otel trace         0.30 MB     ±0.00%        0.30 MB        0.30 MB
# edge log           1.30 MB     ±0.00%        1.30 MB        1.30 MB
#
# Reduction count statistics:
#
# Name               average  deviation         median         99th %
# otel trace         32.09 K     ±0.00%        32.09 K        32.09 K
# edge log          138.71 K     ±0.00%       138.71 K       138.71 K

# Results with flattening logic (clean + flatten as separate passes)
#
# Name                 ips        average  deviation         median         99th %
# otel trace       10.12 K       98.85 μs     ±8.04%       96.88 μs      123.75 μs
# edge log          2.61 K      382.76 μs    ±11.07%      373.33 μs      509.76 μs
#
# Memory usage statistics:
#
# Name               average  deviation         median         99th %
# otel trace         0.31 MB     ±0.00%        0.31 MB        0.31 MB
# edge log           1.34 MB     ±0.00%        1.34 MB        1.34 MB
#
# Reduction count statistics:
#
# Name               average  deviation         median         99th %
# otel trace         32.78 K     ±0.13%        32.76 K        32.89 K
# edge log          140.89 K     ±0.00%       140.89 K       140.89 K
