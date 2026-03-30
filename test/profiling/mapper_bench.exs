import Logflare.Factory

alias Logflare.Backends.Adaptor.ClickHouseAdaptor.MappingDefaults
alias Logflare.LogEvent
alias Logflare.Mapper

user = insert(:user)
source = insert(:source, user: user)

# Semi-realistic edge log payload with deeply nested metadata.
log_params = %{
  "event_message" =>
    "POST | 404 | 33.254.251.15 | 9ee9a1c5fdfa3b4b | https://zzzenjkohrkaatgpywnz.supabase.co/rest/v1/rpc/set_active_session | Deno/2.1.4 (variant; SupabaseEdgeRuntime/1.69.25)",
  "headers" => %{
    "cf_cache_status" => "DYNAMIC",
    "cf_ray" => "99f7a1c7c2c33c4e-BOM",
    "content_type" => "application/json; charset=utf-8",
    "date" => "Thu, 15 Jan 2026 17:14:00 GMT",
    "sb_gateway_version" => "1",
    "sb_request_id" => "9f8bc2a6-57c0-7460-d3ad-9cebebcde78e",
    "transfer_encoding" => "chunked"
  },
  "id" => "65a8a593-ba4c-43ef-999f-6aa46a8d46d8",
  "identifier" => "zzzenjkohrkaatgpywnz",
  "logflare_worker" => %{
    "worker_id" => "ENUD7E"
  },
  "metadata" => %{
    "logflare_worker" => %{
      "worker_id" => "ENUD7E"
    },
    "request" => %{
      "cf" => %{
        "asOrganization" => "Amazon Technologies Inc.",
        "asn" => 16_509,
        "botManagement" => %{
          "corporateProxy" => false,
          "ja3Hash" => "8a64967e35f306b9a5f5cfe592dd153e",
          "ja4" => "t93e1011h2_61c7ad8aa9b6_3fcd1a44f789",
          "ja4Signals" => %{
            "browser_ratio_1h" => 0.0026498660445213,
            "cache_ratio_1h" => 0.038907047361135,
            "h2h3_ratio_1h" => 0.98994463682175,
            "heuristic_ratio_1h" => 0.22291173040867,
            "ips_quantile_1h" => 0.99974054098129,
            "ips_rank_1h" => 218,
            "paths_rank_1h" => 16,
            "reqs_quantile_1h" => 0.99995714426041,
            "reqs_rank_1h" => 36,
            "uas_rank_1h" => 294
          },
          "jsDetection" => %{
            "passed" => false
          },
          "score" => 22,
          "staticResource" => false,
          "verifiedBot" => false
        },
        "city" => "Mumbai",
        "clientAcceptEncoding" => "gzip, br",
        "clientTcpRtt" => 1,
        "clientTrustScore" => 22,
        "colo" => "BOM",
        "continent" => "AS",
        "country" => "IN",
        "edgeRequestKeepAliveStatus" => 1,
        "httpProtocol" => "HTTP/2",
        "latitude" => "19.07287",
        "longitude" => "72.88262",
        "postalCode" => "400017",
        "region" => "Maharashtra",
        "regionCode" => "MH",
        "requestPriority" => "weight=16;exclusive=0;group=0;group-weight=0",
        "timezone" => "Asia/Kolkata",
        "tlsCipher" => "AEAD-AES256-GCM-SHA384",
        "tlsClientAuth" => %{
          "certPresented" => "0",
          "certRevoked" => "0",
          "certVerified" => "NONE"
        },
        "tlsClientCiphersSha1" => "WUS3+h7TjVkF8aEouGuCEMjGQHA=",
        "tlsClientExtensionsSha1" => "ROAd8XY7DPwmmnYhpORdf3M19NE=",
        "tlsClientExtensionsSha1Le" => "WANJZpsaEjTvZbhntqQLKduIDgf=",
        "tlsClientHelloLength" => "268",
        "tlsClientRandom" => "ZZ7YaL2fEWTxlUuXPNzXYkjjp7xMySrWXFZWn6Yz1XZ=",
        "tlsExportedAuthenticator" => %{
          "clientFinished" =>
            "7cb587db93afce5a67bfa942c54853c46b2d57f0f66b3603a6d9b8429da18ad8c4a4a54cd15d6bf1fd42e59139d62d62",
          "clientHandshake" =>
            "8bbc12bca6736ce3eb7aa06e6cb06e3e1532cd84de5792473b9dc53a092d01694569c7902917f63dd69f68a064d9ed32",
          "serverFinished" =>
            "cb1f0f4aa08f5667013cda52f8d25e82fb3e19dbefa321668789aef9c0955e731343258ebc6461b8db9649b85d9271c4",
          "serverHandshake" =>
            "cb3bb2098387dd795c33357f0d63c45a85cce502dfa3d1a4cba8e249e8f1313f85dedf40b8e126d2ec58984fd3d7da5d"
        },
        "tlsVersion" => "TLSv1.3"
      },
      "headers" => %{
        "accept" => "*/*",
        "cf_connecting_ip" => "33.254.251.15",
        "cf_ipcountry" => "IN",
        "cf_ray" => "9ee9a1c5fdfa3b4b",
        "content_length" => "158",
        "content_type" => "application/json",
        "host" => "zzzenjkohrkaatgpywnz.supabase.co",
        "user_agent" => "Deno/2.1.4 (variant; SupabaseEdgeRuntime/1.69.25)",
        "x_client_info" => "supabase-js/2.20.0",
        "x_forwarded_proto" => "https",
        "x_real_ip" => "33.254.251.15"
      },
      "host" => "zzzenjkohrkaatgpywnz.supabase.co",
      "method" => "POST",
      "path" => "/rest/v1/rpc/set_active_session",
      "protocol" => "https:",
      "sb" => %{
        "jwt" => %{
          "apikey" => %{
            "payload" => %{
              "algorithm" => "HS256",
              "expires_at" => 2_062_373_547,
              "issuer" => "supabase",
              "role" => "service_role",
              "signature_prefix" => "grnZE8"
            }
          },
          "authorization" => %{
            "payload" => %{
              "algorithm" => "HS256",
              "expires_at" => 2_062_373_547,
              "issuer" => "supabase",
              "role" => "service_role",
              "signature_prefix" => "grnZE8"
            }
          }
        }
      },
      "url" => "https://zzzenjkohrkaatgpywnz.supabase.co/rest/v1/rpc/set_active_session"
    },
    "response" => %{
      "headers" => %{
        "cf_cache_status" => "DYNAMIC",
        "cf_ray" => "99f7a1c7c2c33c4e-BOM",
        "content_type" => "application/json; charset=utf-8",
        "date" => "Thu, 15 Jan 2026 17:14:00 GMT",
        "sb_gateway_version" => "1",
        "sb_request_id" => "9f8bc2a6-57c0-7460-d3ad-9cebebcde78e",
        "transfer_encoding" => "chunked"
      },
      "origin_time" => 367,
      "status_code" => 404
    }
  },
  "origin_time" => 367,
  "project" => "zzzenjkohrkaatgpywnz",
  "request" => %{
    "cf" => %{
      "asOrganization" => "Amazon Technologies Inc.",
      "asn" => 16_509,
      "botManagement" => %{
        "corporateProxy" => false,
        "ja3Hash" => "8a64967e35f306b9a5f5cfe592dd153e",
        "ja4" => "t93e1011h2_61c7ad8aa9b6_3fcd1a44f789",
        "ja4Signals" => %{
          "browser_ratio_1h" => 0.0026498660445213,
          "cache_ratio_1h" => 0.038907047361135,
          "h2h3_ratio_1h" => 0.98994463682175,
          "heuristic_ratio_1h" => 0.22291173040867,
          "ips_quantile_1h" => 0.99974054098129,
          "ips_rank_1h" => 218,
          "paths_rank_1h" => 16,
          "reqs_quantile_1h" => 0.99995714426041,
          "reqs_rank_1h" => 36,
          "uas_rank_1h" => 294
        },
        "jsDetection" => %{
          "passed" => false
        },
        "score" => 22,
        "staticResource" => false,
        "verifiedBot" => false
      },
      "city" => "Mumbai",
      "clientAcceptEncoding" => "gzip, br",
      "clientTcpRtt" => 1,
      "clientTrustScore" => 22,
      "colo" => "BOM",
      "continent" => "AS",
      "country" => "IN",
      "edgeRequestKeepAliveStatus" => 1,
      "httpProtocol" => "HTTP/2",
      "latitude" => "19.07283",
      "longitude" => "72.88261",
      "postalCode" => "400017",
      "region" => "Maharashtra",
      "regionCode" => "MH",
      "requestPriority" => "weight=16;exclusive=0;group=0;group-weight=0",
      "timezone" => "Asia/Kolkata",
      "tlsCipher" => "AEAD-AES256-GCM-SHA384",
      "tlsClientAuth" => %{
        "certPresented" => "0",
        "certRevoked" => "0",
        "certVerified" => "NONE"
      },
      "tlsClientCiphersSha1" => "WUS3+h7TjVkF8aEouGuCEMjGQHA=",
      "tlsClientExtensionsSha1" => "ROAd8XY7DPwmmnYhpORdf3M19NE=",
      "tlsClientExtensionsSha1Le" => "WANJZpsaEjTvZbhntqQLKduIDgf=",
      "tlsClientHelloLength" => "268",
      "tlsClientRandom" => "ZZ7YaL2fEWTxlUuXPNzXYkjjp7xMySrWXFZWn6Yz1XZ=",
      "tlsExportedAuthenticator" => %{
        "clientFinished" =>
          "7cb587db93afce5a67bfa942c54853c46b2d57f0f66b3603a6d9b8429da18ad8c4a4a54cd15d6bf1fd42e59139d62d62",
        "clientHandshake" =>
          "8bbc12bca6736ce3eb7aa06e6cb06e3e1532cd84de5792473b9dc53a092d01694569c7902917f63dd69f68a064d9ed32",
        "serverFinished" =>
          "cb1f0f4aa08f5667013cda52f8d25e82fb3e19dbefa321668789aef9c0955e731343258ebc6461b8db9649b85d9271c4",
        "serverHandshake" =>
          "cb3bb2098387dd795c33357f0d63c45a85cce502dfa3d1a4cba8e249e8f1313f85dedf40b8e126d2ec58984fd3d7da5d"
      },
      "tlsVersion" => "TLSv1.3"
    },
    "headers" => %{
      "accept" => "*/*",
      "cf_connecting_ip" => "33.254.251.15",
      "cf_ipcountry" => "IN",
      "cf_ray" => "9ee9a1c5fdfa3b4b",
      "content_length" => "158",
      "content_type" => "application/json",
      "host" => "zzzenjkohrkaatgpywnz.supabase.co",
      "user_agent" => "Deno/2.1.4 (variant; SupabaseEdgeRuntime/1.69.25)",
      "x_client_info" => "supabase-js/2.20.0",
      "x_forwarded_proto" => "https",
      "x_real_ip" => "33.254.251.15"
    },
    "host" => "zzzenjkohrkaatgpywnz.supabase.co",
    "method" => "POST",
    "path" => "/rest/v1/rpc/set_active_session",
    "protocol" => "https:",
    "sb" => %{
      "jwt" => %{
        "apikey" => %{
          "payload" => %{
            "algorithm" => "HS256",
            "expires_at" => 2_062_373_547,
            "issuer" => "supabase",
            "role" => "service_role",
            "signature_prefix" => "grnZE8"
          }
        },
        "authorization" => %{
          "payload" => %{
            "algorithm" => "HS256",
            "expires_at" => 2_062_373_547,
            "issuer" => "supabase",
            "role" => "service_role",
            "signature_prefix" => "grnZE8"
          }
        }
      }
    },
    "url" => "https://zzzenjkohrkaatgpywnz.supabase.co/rest/v1/rpc/set_active_session"
  },
  "request_id" => "9f8bc2a6-57c0-7460-d3ad-9cebebcde78e",
  "response" => %{
    "headers" => %{
      "cf_cache_status" => "DYNAMIC",
      "cf_ray" => "99f7a1c7c2c33c4e-BOM",
      "content_type" => "application/json; charset=utf-8",
      "date" => "Thu, 15 Jan 2026 17:14:00 GMT",
      "sb_gateway_version" => "1",
      "sb_request_id" => "9f8bc2a6-57c0-7460-d3ad-9cebebcde78e",
      "transfer_encoding" => "chunked"
    },
    "origin_time" => 367,
    "status_code" => 404
  },
  "source" => "985df630-a551-4c79-ff27-042650b37c62",
  "status_code" => 404,
  "timestamp" => 1_768_497_240_000_000
}

# ── Build LogEvent via standard ingestion path ───────────────────────────
log_event = LogEvent.make(log_params, %{source: source})

# ── Compile mapping config ───────────────────────────────────────────────
log_compiled = Mapper.compile!(MappingDefaults.for_log())

# ── Verify output ────────────────────────────────────────────────────────
log_result = Mapper.map(log_event.body, log_compiled)

# credo:disable-for-lines:6
IO.puts("\n--- Log mapping (body) ---")
IO.puts("Output keys: #{inspect(Map.keys(log_result) |> Enum.sort())}")
IO.puts("resource_attributes: #{inspect(log_result["resource_attributes"])}")
IO.puts("log_attributes key count: #{map_size(log_result["log_attributes"])}")
IO.puts("")

# ── Benchmark ────────────────────────────────────────────────────────────
Benchee.run(
  %{
    "[log] Mapper.map(body)" => fn -> Mapper.map(log_event.body, log_compiled) end
  },
  time: 5,
  warmup: 2,
  memory_time: 3,
  reduction_time: 3
)

# Baseline results — Mapper.map(event.body) with MappingDefaults.for_log()
# Apple M4 / 32 GB / macOS / Elixir 1.19.5 / Erlang 27.3.4.6
#
# Name                             ips        average  deviation         median         99th %
# [log] Mapper.map(body)       17.09 K       58.53 μs    ±32.69%          49 μs      132.46 μs
#
# Memory usage statistics:
#
# Name                      Memory usage
# [log] Mapper.map(body)         5.90 KB
#
# Reduction count statistics:
#
# Name                   Reduction count
# [log] Mapper.map(body)             637
