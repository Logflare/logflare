---
title: ClickHouse query-class policy
---

# ClickHouse query-class policy

Operators can configure scheduling and resource limits independently of ClickHouse read-cluster routing. The backend configuration key is `query_class_settings`. It is disabled by default; an empty map removes the class policy without removing endpoint-owned limits.

## Operator configuration

Use the internal operator function with a persisted admin user. The admin flag is provisioned out of band and rechecked on every save. Customer backend changesets, the backend API, and customer forms cannot introduce, change, or clear this configuration. Public backend JSON omits it. An operator must clear this policy before changing backend type. Ordinary saves merge under a row lock so stale customer snapshots cannot erase newer operator policy. No new customer-supplied ClickHouse settings are enabled.

Endpoint-owned policy also requires a persisted admin `User`. Use the administrator's own account context, not a team-member context: matching email addresses or an admin team owner grant no operator privileges. Team profile edits can change display/contact preferences, not authentication identity.

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

The header is a scheduling hint, not an authorization boundary. Selecting a label does not grant access to another tenant's data.

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

Endpoint-owned policy is passed separately from consumer SQL. Consumer SQL cannot override enforced keys, including in nested SETTINGS clauses. The adaptor injects the combined policy once. Supported `EXPLAIN SELECT` wrappers are preserved. Previews show only endpoint-owned settings, not operator class policy. The actual request class and backend policy obtained through the metadata cache are applied at execution time, subject to the consistency limitations below.

Recognized maintenance statements on the raw adaptor path, such as `SYSTEM FLUSH LOGS`, `EXISTS TABLE`, and DDL, pass through without class settings. They retain the connection's existing ClickHouse permissions; this does not grant additional privileges. Endpoint SQL remains SELECT-only, and endpoint and Ecto-generated read paths retain policy enforcement.

Historical endpoint versions retain current endpoint-owned limits. Cached snapshot reloads also overlay current endpoint policy. Saving endpoint-owned limits invalidates caches for all endpoint versions and stops their active refresh tasks. Already-running foreground queries finish for their existing callers; new callers use replacement caches with current settings.

Successful result caches remain class-agnostic: changing the class does not create a different result-cache key or run a query on a cache hit. Misses and background refreshes enforce policy at execution time. A refresh retains the class from the request that originally created the cache and uses backend policy obtained through the metadata cache, subject to the consistency limitations below. Backend class-policy changes invalidate backend metadata across the cluster, but do not evict reusable successful results, restart ingesters, or tear down active read connections.

## Policy update consistency

Backend query-class policy updates are eventually consistent across Logflare instances. A successful save confirms that the configuration was persisted, but does not guarantee that every subsequent query immediately uses it. In-flight cache fills and read-replica lag can temporarily preserve the previous policy, including after limits are tightened, relaxed, or removed.

Do not rely on these updates for immediate emergency throttling or hard resource guarantees. Use ClickHouse query-user profile constraints for hard ceilings. During rollout or rollback, verify effective settings using `system.query_log.Settings` across the relevant workloads and read clusters. Cache expiry is not a guaranteed propagation deadline. This limitation concerns backend class-policy updates, not the endpoint-owned limit invalidation described above.

Endpoint-owned policy misses share a fill per endpoint, with primary reads outside the shared cache transaction queue. Invalidation advances a generation and evicts the entry without waiting for primary reads. Superseded fills retry instead of republishing old settings.

If an endpoint-owned policy is not cached and its primary read fails, or a current policy cannot be established within three attempts, execution fails closed with a retryable HTTP 503. Editor Run displays retry guidance instead of disconnecting. Already-cached policies remain usable. During rolling deploys, eviction skips nodes predating the endpoint-policy invalidation API because they cannot hold those cache entries; this does not make it safe to enable policy before all instances support enforcement.

## Deployment and verification

1. Using the backend's actual query credentials, test each configured setting with a small SELECT. Confirm intended values are accepted and values outside profile constraints are rejected. Keep query-user permissions restricted; do not weaken `readonly` or grant DDL to enable policy. See [ClickHouse setting constraints](https://clickhouse.com/docs/operations/settings/constraints-on-settings).
2. Deploy support on every Logflare instance and drain older versions before enabling policy. Coordinate any ClickHouse profile changes with the backend policy; do not change profile defaults first. Use profile constraints for hard ceilings and consider `priority MIN 1` to prevent queries from opting out of prioritization.
3. Enable query-setting logging and inspect `system.query_log.Settings` using query IDs. Verify priorities and limits for configured classes, missing or unknown labels, and requests routed to the default cluster.
4. Load-test competing workloads in staging, then monitor latency, timeouts, query errors, memory, and connections during rollout. Lower-priority queries can retain resources while paused; priority does not provide tenant isolation or aggregate throttling.

To roll back class policy, pass `%{}` to `Logflare.Backends.configure_query_class_settings/3`. Endpoint-owned limits remain active. Coordinate any profile rollback with requests and instances still using class policy.
