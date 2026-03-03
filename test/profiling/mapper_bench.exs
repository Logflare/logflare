alias Logflare.Mapper
alias Logflare.Mapper.MappingConfig
alias Logflare.Mapper.MappingConfig.FieldConfig, as: Field

# Real-world edge log payload (POST 404) with deeply nested metadata.
# Source: Supabase edge function log via Cloudflare worker.
payload = %{
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

# Mapping config aligned with the OTEL logs table DDL
logs_mapping =
  MappingConfig.new([
    Field.string("project",
      paths: ["$.project", "$.project_ref", "$.project_id"],
      default: ""
    ),
    Field.string("trace_id",
      paths: ["$.trace_id", "$.traceId", "$.otel_trace_id"],
      default: ""
    ),
    Field.string("span_id",
      paths: ["$.span_id", "$.spanId", "$.otel_span_id"],
      default: ""
    ),
    Field.uint8("trace_flags",
      paths: ["$.trace_flags", "$.traceFlags"],
      default: 0
    ),
    Field.string("severity_text",
      paths: ["$.severity_text", "$.severityText", "$.metadata.level", "$.level"],
      default: "INFO",
      transform: "upcase",
      allowed_values:
        ~w(TRACE DEBUG INFO NOTICE WARN WARNING ERROR FATAL CRITICAL EMERGENCY ALERT LOG PANIC)
    ),
    Field.uint8("severity_number",
      from_output: "severity_text",
      value_map: %{
        "TRACE" => 1,
        "DEBUG" => 5,
        "INFO" => 9,
        "WARN" => 13,
        "WARNING" => 13,
        "ERROR" => 17,
        "FATAL" => 21,
        "CRITICAL" => 21,
        "EMERGENCY" => 21
      },
      default: 0
    ),
    Field.string("service_name",
      paths: [
        "$.resource.service.name",
        "$.service_name",
        "$.resource.name",
        "$.metadata.context.application"
      ],
      default: ""
    ),
    Field.string("event_message",
      paths: ["$.event_message", "$.message", "$.body", "$.msg"],
      default: ""
    ),
    Field.string("scope_name",
      paths: [
        "$.scope.name",
        "$.metadata.context.module",
        "$.metadata.context.application",
        "$.instrumentation_library.name"
      ],
      default: ""
    ),
    Field.string("scope_version",
      paths: [
        "$.scope.version",
        "$.instrumentation_library.version"
      ],
      default: ""
    ),
    Field.string("scope_schema_url",
      paths: ["$.scope.schema_url"],
      default: ""
    ),
    Field.string("resource_schema_url",
      paths: ["$.resource.schema_url"],
      default: ""
    ),
    Field.json("resource_attributes",
      paths: ["$.resource"],
      pick: [
        {"region", ["$.metadata.region", "$.region"]},
        {"cluster", ["$.metadata.cluster", "$.cluster"]},
        {"service.name", ["$.resource.service.name", "$.service_name"]},
        {"application", ["$.metadata.context.application"]},
        {"node", ["$.metadata.context.vm.node"]},
        {"project", ["$.project", "$.project_ref", "$.project_id"]}
      ],
      default: %{}
    ),
    Field.json("scope_attributes",
      paths: ["$.scope.attributes", "$.scope"],
      default: %{}
    ),
    Field.json("log_attributes",
      path: "$",
      exclude_keys: ["id", "event_message", "timestamp"],
      elevate_keys: ["metadata"]
    ),
    Field.datetime64("timestamp", path: "$.timestamp", precision: 9)
  ])

compiled = Mapper.compile!(logs_mapping)
nif_result = Mapper.map(payload, compiled)

# credo:disable-for-lines:3
IO.puts("\n--- Log scenario ---")
IO.puts("Output Keys: #{inspect(Map.keys(nif_result) |> Enum.sort())}")
IO.puts("")

# ── Array extraction scenario ─────────────────────────────────────────
#
# Simulates an OTEL histogram metric data point with parallel arrays,
# exemplars (list of objects → wildcard extraction), and nested attributes.

metric_payload = %{
  "name" => "http.server.request.duration",
  "timestamp" => 1_769_018_088_144_506,
  "start_time" => 1_769_018_080_000_000,
  "count" => 142,
  "sum" => 8472.5,
  "min" => 0.3,
  "max" => 892.1,
  "explicit_bounds" => [0.0, 5.0, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0, 750.0, 1000.0],
  "bucket_counts" => [0, 12, 28, 45, 30, 15, 8, 3, 1, 0, 0, 0],
  "exemplars" => [
    %{
      "trace_id" => "abc123def456",
      "span_id" => "span001",
      "value" => 12.5,
      "timestamp" => 1_769_018_085_000_000,
      "attributes" => %{"http.method" => "GET", "http.status_code" => 200}
    },
    %{
      "trace_id" => "fed654cba321",
      "span_id" => "span002",
      "value" => 892.1,
      "timestamp" => 1_769_018_087_500_000,
      "attributes" => %{"http.method" => "POST", "http.status_code" => 500}
    },
    %{
      "trace_id" => "111222333444",
      "span_id" => "span003",
      "value" => 3.2,
      "timestamp" => 1_769_018_088_100_000,
      "attributes" => %{"http.method" => "GET", "http.status_code" => 200}
    }
  ],
  "resource" => %{
    "service" => %{"name" => "api-gateway"},
    "region" => "us-east-1"
  },
  "scope" => %{"name" => "otel-elixir", "version" => "1.4.0"},
  "attributes" => %{
    "http.method" => "GET",
    "http.route" => "/api/v1/users",
    "http.scheme" => "https"
  },
  "aggregation_temporality" => "cumulative"
}

metric_mapping =
  MappingConfig.new([
    Field.string("metric_name", path: "$.name", default: ""),
    Field.datetime64("timestamp", path: "$.timestamp", precision: 9),
    Field.datetime64("start_time", path: "$.start_time", precision: 9),
    Field.uint64("count", path: "$.count", default: 0),
    Field.float64("sum", path: "$.sum", default: 0.0),
    Field.float64("min", path: "$.min", default: 0.0),
    Field.float64("max", path: "$.max", default: 0.0),
    Field.array_float64("explicit_bounds", path: "$.explicit_bounds"),
    Field.array_uint64("bucket_counts", path: "$.bucket_counts"),
    Field.array_string("exemplar_trace_ids", path: "$.exemplars[*].trace_id"),
    Field.array_string("exemplar_span_ids", path: "$.exemplars[*].span_id"),
    Field.array_float64("exemplar_values", path: "$.exemplars[*].value"),
    Field.array_datetime64("exemplar_timestamps",
      path: "$.exemplars[*].timestamp",
      precision: 9
    ),
    Field.array_map("exemplar_attributes", path: "$.exemplars[*].attributes"),
    Field.json("resource_attributes", path: "$.resource"),
    Field.json("scope_attributes", path: "$.scope"),
    Field.json("metric_attributes", path: "$.attributes")
  ])

compiled_metric = Mapper.compile!(metric_mapping)
nif_metric_result = Mapper.map(metric_payload, compiled_metric)

# credo:disable-for-lines:3
IO.puts("\n--- Metric (array) scenario ---")
IO.puts("Output Keys: #{inspect(Map.keys(nif_metric_result) |> Enum.sort())}")
IO.puts("")

# ── FlatMap scenarios (simple schema equivalents) ─────────────────────

alias Logflare.Backends.Adaptor.ClickHouseAdaptor.MappingDefaults

simple_log_compiled = Mapper.compile!(MappingDefaults.for_log_simple())
simple_log_result = Mapper.map(payload, simple_log_compiled)

simple_metric_compiled = Mapper.compile!(MappingDefaults.for_metric_simple())
simple_metric_result = Mapper.map(metric_payload, simple_metric_compiled)

# credo:disable-for-lines:8
IO.puts("\n--- Simple log (flat_map) scenario ---")
IO.puts("Output Keys: #{inspect(Map.keys(simple_log_result) |> Enum.sort())}")
IO.puts("resource_attributes type: #{inspect(simple_log_result["resource_attributes"])}")
IO.puts("")
IO.puts("\n--- Simple metric (flat_map) scenario ---")
IO.puts("Output Keys: #{inspect(Map.keys(simple_metric_result) |> Enum.sort())}")
IO.puts("resource_attributes type: #{inspect(simple_metric_result["resource_attributes"])}")
IO.puts("")

Benchee.run(
  %{
    "[log] JSON mapping" => fn -> Mapper.map(payload, compiled) end,
    "[log] FlatMap mapping" => fn -> Mapper.map(payload, simple_log_compiled) end,
    "[metric] JSON mapping" => fn ->
      Mapper.map(metric_payload, compiled_metric)
    end,
    "[metric] FlatMap mapping" => fn ->
      Mapper.map(metric_payload, simple_metric_compiled)
    end
  },
  time: 5,
  warmup: 2,
  memory_time: 3,
  reduction_time: 3
)
