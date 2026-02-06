# Baseline benchmark for LogEvent.make/2 across realistic payload shapes.
#
# Covers 7 real-world payloads: 3 logs, 3 traces, 1 metric.
# Run with: mix run test/profiling/log_event_make_type_detection_bench.exs

import Logflare.Factory

alias Logflare.LogEvent

user = insert(:user)
source = insert(:source, user: user)

# ============================================================================
# Payload generators - realistic shapes for each log type
# ============================================================================

defmodule PayloadGenerator do
  @moduledoc false

  # -- Logs ------------------------------------------------------------------

  def http_cloudflare_log do
    %{
      "event_message" =>
        "GET | 101 | #{random_ip()} | #{random_hex(16)} | https://example.supabase.co/realtime/v1/websocket?apikey=REDACTED&vsn=1.0.0 | Mozilla/5.0 Chrome/143.0.0.0",
      "identifier" => "zzziycjwigazzxfizzzu",
      "metadata" => %{
        "logflare_worker" => %{"worker_id" => "ZZK2ZW"},
        "request" => %{
          "cf" => %{
            "asOrganization" => "Example ISP",
            "asn" => 3209,
            "botManagement" => %{
              "corporateProxy" => false,
              "ja3Hash" => random_hex(32),
              "ja4" => "t72d1516h1_8dbaf6152771_ee365e64def3",
              "ja4Signals" => %{
                "browser_ratio_1h" => 0.80395168066025,
                "cache_ratio_1h" => 0.051449675112963,
                "h2h3_ratio_1h" => 0,
                "heuristic_ratio_1h" => 0.0016917433822528,
                "ips_quantile_1h" => 0.99995476007462,
                "ips_rank_1h" => 38,
                "paths_rank_1h" => 84,
                "reqs_quantile_1h" => 0.99990123510361,
                "reqs_rank_1h" => 83,
                "uas_rank_1h" => 60
              },
              "jsDetection" => %{"passed" => false},
              "score" => 71,
              "staticResource" => false,
              "verifiedBot" => false
            },
            "city" => "Berlin",
            "clientAcceptEncoding" => "gzip, deflate, br",
            "clientTcpRtt" => 14,
            "clientTrustScore" => 71,
            "colo" => "TXL",
            "continent" => "EU",
            "country" => "DE",
            "edgeRequestKeepAliveStatus" => 1,
            "httpProtocol" => "HTTP/1.1",
            "latitude" => "52.52436",
            "longitude" => "13.41059",
            "postalCode" => "10119",
            "region" => "State of Berlin",
            "regionCode" => "BE",
            "timezone" => "Europe/Berlin",
            "tlsCipher" => "AEAD-AES128-GCM-SHA256",
            "tlsClientAuth" => %{
              "certPresented" => "0",
              "certRevoked" => "0",
              "certVerified" => "NONE"
            },
            "tlsClientCiphersSha1" => "ZkWWX+BVdX+teLUzccOBfetwATE=",
            "tlsClientExtensionsSha1" => "ZkztSobBRnd0+tDSCsq2V9gUYoM=",
            "tlsClientExtensionsSha1Le" => "ZeGtu4/zZCrvR+99enO3apRrXuA=",
            "tlsClientHelloLength" => "2024",
            "tlsClientRandom" => "qqZz6socD4dXPpa+8SdWCP+tTydT7QoqYHD7Ln7O2VE=",
            "tlsExportedAuthenticator" => %{
              "clientFinished" => random_hex(65),
              "clientHandshake" => random_hex(64),
              "serverFinished" => random_hex(64),
              "serverHandshake" => random_hex(64)
            },
            "tlsVersion" => "TLSv1.3"
          },
          "headers" => %{
            "cf_connecting_ip" => random_ip(),
            "cf_ipcountry" => "DE",
            "cf_ray" => random_hex(16),
            "host" => "example.supabase.co",
            "user_agent" => "Mozilla/5.0 Chrome/143.0.0.0",
            "x_forwarded_proto" => "https",
            "x_real_ip" => random_ip()
          },
          "host" => "example.supabase.co",
          "method" => "GET",
          "path" => "/realtime/v1/websocket",
          "protocol" => "https:",
          "search" => "?apikey=REDACTED&vsn=1.0.0",
          "url" => "https://example.supabase.co/realtime/v1/websocket?apikey=REDACTED&vsn=1.0.0"
        },
        "response" => %{
          "headers" => %{
            "cf_cache_status" => "DYNAMIC",
            "cf_ray" => random_hex(16) <> "-TXL",
            "date" => "Tue, 16 Dec 2025 18:39:44 GMT",
            "sb_gateway_mode" => "direct",
            "sb_gateway_version" => "1",
            "sb_request_id" => random_uuid()
          },
          "origin_time" => 69,
          "status_code" => 101
        }
      },
      "project" => "zzziycjwigazzxfizzzu",
      "request_id" => random_uuid(),
      "source" => random_uuid()
    }
  end

  def observer_system_log do
    %{
      "cluster" => "prod-c",
      "event_message" => "Observer metrics!",
      "metadata" => %{
        "cluster" => "prod-c",
        "context" => %{
          "application" => "logflare",
          "domain" => "elixir",
          "file" => "lib/logflare/system_metrics/observer.ex",
          "function" => "dispatch_stats/0",
          "gl" => "<0.6525.0>",
          "line" => 9,
          "mfa" => [
            "Elixir.Logflare.SystemMetrics.Observer",
            "dispatch_stats",
            "0"
          ],
          "module" => "Elixir.Logflare.SystemMetrics.Observer",
          "pid" => "<0.11601.0>",
          "time" => 1_767_114_522_446_384,
          "vm" => %{"node" => "logflare-prod-c@10.156.5.67"}
        },
        "level" => "info",
        "observer_memory" => %{
          "atom" => 3,
          "atom_used" => 3,
          "binary" => 626,
          "code" => 85,
          "ets" => 280,
          "processes" => 9148,
          "processes_used" => 9138,
          "system" => 1863,
          "total" => 11_012
        },
        "observer_metrics" => %{
          "atom_count" => 119_531,
          "atom_limit" => 32_000_000,
          "ets_count" => 5785,
          "ets_limit" => 250_000,
          "io_input" => 10_455_148_310_781,
          "io_output" => 5_604_158_809_146,
          "logical_processors" => 32,
          "logical_processors_available" => 32,
          "logical_processors_online" => 32,
          "otp_release" => "27",
          "port_count" => 2307,
          "port_limit" => 1_048_576,
          "process_count" => 82_914,
          "process_limit" => 8_388_608,
          "run_queue" => 0,
          "schedulers" => 32,
          "schedulers_available" => 32,
          "schedulers_online" => 32,
          "total_active_tasks" => 6,
          "uptime" => 941_887_641,
          "version" => "15.2.7.2"
        }
      }
    }
  end

  def phoenix_log do
    %{
      "event_message" => "Sent 202 in 23ms",
      "metadata" => %{
        "cluster" => "main",
        "context" => %{
          "application" => "phoenix",
          "domain" => ["elixir"],
          "file" => "lib/phoenix/logger.ex",
          "function" => "phoenix_endpoint_stop/4",
          "gl" => "<0.3288.0>",
          "line" => 231,
          "mfa" => ["Elixir.Phoenix.Logger", "phoenix_endpoint_stop", "4"],
          "module" => "Elixir.Phoenix.Logger",
          "pid" => "<0.232639498.0>",
          "time" => 1_765_916_761_127_324,
          "vm" => %{
            "node" => "realtime@2901:da1c:2ce:df01:16ea:929d:f3d5:5c31"
          }
        },
        "external_id" => "zzzsgpmqvexirxfgtqab",
        "level" => "info",
        "otel_span_id" => random_hex(16),
        "otel_trace_flags" => "00",
        "otel_trace_id" => random_hex(32),
        "project" => "zzzsgpmqvexirxfgtqab",
        "region" => "eu-west-2",
        "request_id" => "ZJHL9CCEtzk0AHgirc0O"
      },
      "project" => "zzzsgpmqvexirxfgtqab"
    }
  end

  # -- Traces ----------------------------------------------------------------

  def auth_trace_span do
    %{
      "attributes" => %{
        "_http_client_ip" => random_ip(),
        "_http_method" => "POST",
        "_http_request_content_length" => 1724,
        "_http_response_content_length" => 68,
        "_http_route" => "/token",
        "_http_scheme" => "http",
        "_http_status_code" => 400,
        "_http_target" => "/token",
        "_net_host_name" => "auth.supabase.io",
        "_net_protocol_version" => "1.1",
        "_net_sock_peer_addr" => random_ip(),
        "_net_sock_peer_port" => 36_196,
        "_user_agent_original" => "stripped"
      },
      "end_time" => "2025-12-22T20:04:25.511409Z",
      "event_message" => "api",
      "metadata" => %{"type" => "span"},
      "resource" => %{
        "gotrue" => %{"version" => "v2.184.0-rc.5"},
        "supabase" => %{
          "read_replica" => "false",
          "region" => "ap-southeast-1",
          "stack" => "prod"
        }
      },
      "scope" => %{
        "name" => "go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp",
        "version" => "0.51.0"
      },
      "span_id" => random_hex(16),
      "start_time" => "2025-12-22T20:04:25.369201Z",
      "trace_id" => random_hex(32)
    }
  end

  def api_gateway_trace_span do
    %{
      "attributes" => %{
        "_client_address" => "2600:1fc4:22a5:ae73:2887:684b:66c0:1f85",
        "_http_request_method" => "POST",
        "_http_route" => "/functions/v1/*path",
        "_http_status_code" => 404,
        "_network_peer_address" => random_ip(),
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
        "service" => %{
          "name" => "supabase-api-gateway",
          "version" => "1.0.0"
        }
      },
      "scope" => %{
        "name" => "go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin",
        "version" => "0.61.0"
      },
      "span_id" => random_hex(16),
      "start_time" => "2026-01-21T17:54:48.144506Z",
      "trace_id" => random_hex(32)
    }
  end

  def realtime_trace_span do
    %{
      "attributes" => %{
        "_client_address" => random_ip(),
        "_http_request_method" => "POST",
        "_http_response_status_code" => 202,
        "_http_route" => "/api/broadcast",
        "_network_peer_address" => "::ffff:99.14.9.22",
        "_network_peer_port" => 36_686,
        "_network_protocol_version" => "1.1",
        "_phoenix_action" => "broadcast",
        "_phoenix_plug" => "Elixir.RealtimeWeb.BroadcastController",
        "_server_address" => "azzwtyowylbetupqoxnc.realtime.supabase.co",
        "_url_path" => "/api/broadcast",
        "_url_scheme" => "https",
        "_user_agent_original" => "node",
        "external_id" => "azzwtyowylbetupqoxnc",
        "request_id" => "GIHLNs9DBoiVlWkXjbnC"
      },
      "end_time" => "2025-12-16T20:12:45.173981Z",
      "event_message" => "POST /api/broadcast",
      "metadata" => %{"type" => "span"},
      "parent_span_id" => random_hex(16),
      "project" => "realtime",
      "resource" => %{
        "process" => %{
          "executable" => %{"name" => "realtime"},
          "runtime" => %{
            "description" => "Erlang/OTP 27 erts-15.2.3",
            "name" => "BEAM",
            "version" => "15.2.3"
          }
        },
        "service" => %{
          "instance" => %{
            "id" => "realtime@2600:1f1c:b9a:4901:57ca:a23e:f521:d713"
          },
          "name" => "realtime"
        },
        "telemetry" => %{
          "sdk" => %{
            "language" => "erlang",
            "name" => "opentelemetry",
            "version" => "1.6.0"
          }
        }
      },
      "scope" => %{"name" => "opentelemetry_cowboy", "version" => "1.0.0"},
      "span_id" => random_hex(16),
      "start_time" => "2025-12-16T20:12:45.171505Z",
      "trace_id" => random_hex(32)
    }
  end

  # -- Metrics ---------------------------------------------------------------

  def otel_metric do
    %{
      "aggregation_temporality" => "cumulative",
      "attributes" => %{
        "endpoint_id" => 50,
        "endpoint_uuid" => random_uuid(),
        "project" => "zzzauwrprvhagecudrxy",
        "user_id" => 4499
      },
      "event_message" => "logflare.endpoints.query.total_bytes_processed",
      "is_monotonic" => false,
      "metadata" => %{"type" => "metric"},
      "metric_type" => "sum",
      "project_ref" => "zzzauwrprvhagecudrxy",
      "value" => 25_607_364
    }
  end

  # -- Helpers ---------------------------------------------------------------

  defp random_hex(length) do
    :crypto.strong_rand_bytes(div(length, 2) + 1)
    |> Base.encode16(case: :lower)
    |> binary_part(0, length)
  end

  defp random_uuid do
    Ecto.UUID.generate()
  end

  defp random_ip do
    Enum.map_join(1..4, ".", fn _ -> :rand.uniform(255) end)
  end
end

# ============================================================================
# Pre-generate inputs (isolate LogEvent.make/2 as sole function under test)
# ============================================================================

inputs = %{
  "log: HTTP/Cloudflare" => PayloadGenerator.http_cloudflare_log(),
  "log: Observer/system" => PayloadGenerator.observer_system_log(),
  "log: Phoenix" => PayloadGenerator.phoenix_log(),
  "trace: Auth span" => PayloadGenerator.auth_trace_span(),
  "trace: API gateway" => PayloadGenerator.api_gateway_trace_span(),
  "trace: Realtime (parent)" => PayloadGenerator.realtime_trace_span(),
  "metric: OTEL sum" => PayloadGenerator.otel_metric()
}

# ============================================================================
# Benchmark
# ============================================================================

Benchee.run(
  %{
    "LogEvent.make/2" => fn params ->
      LogEvent.make(params, %{source: source})
    end
  },
  inputs: inputs,
  time: 4,
  warmup: 1,
  memory_time: 3,
  reduction_time: 3
)

# ============================================================================
# Baseline results before log type detection (2026-02-06, Apple M4, 10 cores, 32 GB, OTP 27, Elixir 1.19)
# ============================================================================
#
# Input                       ips        average    memory
# metric: OTEL sum         25.72 K       38.88 μs   111.96 KB
# trace: API gateway        9.76 K      102.43 μs   307.54 KB
# trace: Auth span          9.75 K      102.53 μs   307.23 KB
# log: Phoenix              9.64 K      103.72 μs   293.28 KB
# trace: Realtime (parent)  6.12 K      163.37 μs   515.39 KB
# log: Observer/system      5.07 K      197.35 μs   645.70 KB
# log: HTTP/Cloudflare      1.78 K      562.78 μs     1.47 MB
