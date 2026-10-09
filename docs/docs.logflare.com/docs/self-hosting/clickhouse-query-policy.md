---
title: ClickHouse query-class policy
---

# ClickHouse query-class policy

Operators can configure scheduling and resource limits independently of ClickHouse read-cluster routing. The backend configuration key is `query_class_settings`. It is disabled by default; an empty map removes the class policy without removing endpoint-owned limits.

## Operator configuration

Use the internal operator function with a persisted admin user. The admin flag is provisioned out of band and rechecked on every save. Customer backend changesets, the backend API, and customer forms cannot introduce, change, or clear this configuration. Public backend JSON omits it. An operator must clear this policy before changing backend type. Ordinary saves merge under a row lock so stale customer snapshots cannot erase newer operator policy. No new customer-supplied ClickHouse settings are enabled.

```elixir
admin = Logflare.Users.get(admin_id)
backend = Logflare.Backends.get_backend(backend_id)

settings = %{
  "default" => %{"priority" => 5},
  "dashboard_logs_free" => %{"priority" => 1},
  "dashboard_logs_paid" => %{"priority" => 1},
  "dashboard_reports_free" => %{"priority" => 1},
  "dashboard_reports_paid" => %{"priority" => 1},
  "dashboard_observability" => %{"priority" => 1},
  "api_free" => %{"priority" => 10, "max_threads" => 4},
  "api_paid" => %{"priority" => 10, "max_threads" => 4},
  "mcp" => %{"priority" => 10, "max_threads" => 4}
}

{:ok, backend} =
  Logflare.Backends.configure_query_class_settings(admin, backend, settings)
```

These values are an example, not production configuration. Choose them after checking the deployed query-user profile and load-testing in staging.

A nonempty configuration must include a `default` with a positive integer `priority`. A class can inherit its priority from this default. Missing, unknown, or unconfigured labels receive the default policy. Only the classes listed above are accepted at save time.

The class is selected from the original `LF-ENDPOINT-CLICKHOUSE-READ-CLUSTER-LABEL` request header, even when routing resolves to another cluster or retries on the default cluster. Known query-class labels do not generate unconfigured-routing warnings; unknown routing labels still do.

The header is a scheduling hint, not an authorization boundary. Preserve the existing endpoint authentication, and ensure the trusted Management API classifier supplies the header rather than forwarding an untrusted client's value. Selecting a label does not grant access to another tenant's data.

## Allowed settings and precedence

| Setting | Accepted value |
| --- | --- |
| `priority` | Positive integer; 1 is highest priority, larger values are lower priority |
| `max_threads` | Positive integer |
| `max_memory_usage` | Positive integer bytes |
| `max_bytes_to_read` | Positive integer bytes |
| `max_rows_to_read` | Positive integer rows |
| `max_execution_time` | Positive integer or fractional seconds |
| `read_overflow_mode` | Only `throw`; inserted automatically with byte or row limits |
| `timeout_overflow_mode` | Only `throw`; inserted automatically with an execution-time limit |

Zero (unlimited or no priority), negative values, wrong types, unknown keys, and overflow modes returning partial results are rejected. String values are serialized only from the fixed enum allowlist. Output-format, join semantics, sandbox permissions, and external table/URL settings are not allowed.

ClickHouse profile defaults supply settings not specified by Logflare. Logflare combines class default, selected class, and endpoint-owned settings. Resource limits use the smallest configured value across all three policies; non-limit settings such as priority use the later policy. Hard profile ceilings must be ClickHouse constraints, not merely default values. The server still enforces those constraints after merging.

Endpoint-owned policy is passed separately from consumer SQL. The adaptor checks for consumer overrides anywhere in the final AST, then injects the combined policy once. Supported `EXPLAIN SELECT` wrappers are preserved. Previews use the same policy builder:

```elixir
Logflare.Endpoints.get_transformed_query(endpoint, params, read_cluster: "api_paid")
```

Historical endpoint versions retain current operator ceilings. Cached snapshot reloads also overlay current endpoint policy. Saving endpoint-owned limits invalidates caches for all endpoint versions and stops their active refresh tasks.

Successful result caches remain class-agnostic: changing the class does not create a different result-cache key or run a query on a cache hit. Misses and background refreshes enforce policy at execution time. A refresh retains the class from the request that originally created the cache, but reads current backend policy. Backend class-policy changes invalidate backend metadata across the cluster, but do not evict reusable successful results, restart ingesters, or tear down active read connections.

## Deployment preflight

Before enabling this policy on a backend, connect using its actual query credentials, including any dedicated query user. Inspect the deployed settings and constraints:

```sql
SELECT name, type, value, min, max, readonly
FROM system.settings
WHERE name IN (
  'readonly', 'priority', 'max_threads', 'max_execution_time',
  'max_memory_usage', 'max_bytes_to_read', 'max_rows_to_read',
  'read_overflow_mode', 'timeout_overflow_mode'
);
```

For every newly used key, issue a small SELECT with the intended setting using those credentials. Verify both the accepted values and rejection of values outside profile constraints. `readonly = 1` can prohibit changes unless the setting is specifically changeable in readonly mode; `readonly = 2` allows settings changes subject to constraints. Do not weaken `readonly`, grant DDL, or make `readonly` itself changeable to make these probes pass. See [ClickHouse setting constraints](https://clickhouse.com/docs/operations/settings/constraints-on-settings).

Local regression tests exercise every allowlisted key with a temporary `readonly = 2` user and profile ceilings. They do not establish that the production `logflare_prod` profile permits these values. Production verification is an operator rollout requirement.

Deploy Logflare support on every instance and drain older versions before enabling the policy. Apply the backend class configuration and a matching `priority = 5` baseline in the ClickHouse `logflare` profile in the same coordinated rollout window, **not the profile change before Logflare class policy**. Consider `priority MIN 1` to prevent bypass paths from opting out with zero. Ensure configured limits fit the profile's constraints; use server-side MAX constraints where cluster-wide ceilings are required.

## Verification and rollout monitoring

Ensure query-setting logging is enabled for the verification queries. For actual API/MCP, dashboard, missing-label, and unknown-label requests, inspect `system.query_log.Settings` on Logflare Reads and verify priority and limits, including requests routed to the default cluster. Use the query ID to identify the request; a small local regression test verifies the recorded API values.

In staging, compare dashboard p95 latency and timeout rates under competing API load. During rollout, monitor query errors, delayed concurrency slots, paused queries, memory, and connections. Lower-priority queries retain memory and connections while paused and may see more execution-time failures. Priority is not tenant isolation or aggregate admission control.

Aggregate Management API throttling (O11Y-2662) and moving API/MCP classes to separately sized compute remain separate mitigations. Roll back class policy through the admin function with `%{}`; coordinate any profile rollback after accounting for requests and instances still using class policy.
