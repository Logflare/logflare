---
title: ClickHouse query-class policy
---

# ClickHouse query-class policy

Backend owners and team members can configure scheduling and resource limits independently of ClickHouse read-cluster routing. The backend configuration key is `query_class_settings`. It is disabled by default; an empty map removes the class policy without removing endpoint-owned limits.

## Backend editor

Open an existing ClickHouse backend's edit page and use **Query-class settings** below the connection form. Backend owners and team members with backend access can save or disable this policy. Each save rechecks the persisted actor and current backend ownership; a revoked membership cannot save through an already-open editor. Platform admins retain internal cross-account configuration access.

Set a positive **Default policy** priority, then expand individual classes to configure overrides. Priority 1 is highest; larger numbers are lower priority. The form supports priority, maximum threads, memory usage in bytes, execution time in seconds (including fractions), and rows/bytes scanned. Only execution time can be fractional; whole-number fields also accept equivalent forms such as `4.0` and `1e2`. Blank numeric class fields inherit defaults. A class cannot relax a stricter default resource limit. The editor indicates when an existing filesystem-cache override is present and preserves it on ordinary edits. Use **Reset class to defaults**, then save, to remove all overrides for that class. Numeric limits automatically set the corresponding overflow modes. Clearing a numeric limit keeps any existing overflow mode, whether it was set explicitly or added automatically. Reset an individual class or disable the policy to remove those modes in the editor.

Use **Save query-class settings** to update only this policy, without saving connection changes or restarting ingestion. **Disable query-class policy** clears class settings but leaves endpoint limits and ClickHouse profile constraints in place. If someone changed the policy while the editor was open, reload and reapply your changes before saving or disabling. If the editor reconnects, unsaved changes are discarded so they cannot overwrite a newer policy. Updates propagate eventually across instances, so this editor is not an immediate emergency-throttling control.

This permission change applies only to backend query-class settings. Endpoint-owned policy still requires a persisted admin `User`. Use the administrator's own account context, not a team-member context: matching email addresses or an admin team owner grant no operator privileges. Team profile edits can change display/contact preferences, not authentication identity. Separately, [lower-only settings in endpoint SQL](#lower-only-settings-in-endpoint-sql) can tighten inherited limits or lower scheduling urgency without changing the administrator-enforced policy.

## Internal configuration

`configure_query_class_settings/3` accepts an owner, team-member actor, or persisted platform admin. The backend API and ordinary backend changesets still cannot introduce, change, or clear this configuration; use the dedicated editor or internal function. Public backend JSON continues to omit it. Clear the policy before changing backend type. Ordinary saves merge under a row lock so stale connection-form snapshots cannot erase newer query-class settings. The ClickHouse settings allowlist is unchanged.

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
| `max_execution_time` | Positive integer or fractional seconds; backend policy requires at least one microsecond and must fit in a signed 64-bit microsecond duration |
| `read_overflow_mode` | Only `throw`; inserted automatically with byte or row limits |
| `timeout_overflow_mode` | Only `throw`; inserted automatically with an execution-time limit |

Zero (unlimited or no priority), negative values, wrong types, unknown keys, and overflow modes returning partial results are rejected. String values are serialized only from the fixed enum allowlist. Output-format, join semantics, sandbox permissions, and external table/URL settings are not allowed.

ClickHouse profile defaults supply settings not specified by Logflare. Logflare combines class default, selected class, and endpoint-owned settings. Resource limits use the smallest configured value across all three policies; non-limit settings such as priority use the later policy. Hard profile ceilings must be ClickHouse constraints, not merely default values. The server still enforces those constraints after merging. Legacy backend timeouts outside the supported range are ignored when resolving policy so they cannot override a valid timeout. The editor flags these saved values next to the affected field; replace or clear the value and save to remove the warning.

Administrator-enforced endpoint policy is passed separately from consumer SQL. Endpoint SQL can request only the lower-only settings described below; all other conflicts with enforced keys remain rejected, including in nested SETTINGS clauses. The adaptor injects the combined policy once. Supported `EXPLAIN SELECT` wrappers are preserved. The actual request class and backend policy obtained through the metadata cache are applied at execution time, subject to the consistency limitations below.

Recognized maintenance statements on the raw adaptor path, such as `SYSTEM FLUSH LOGS`, `EXISTS TABLE`, and DDL, pass through without class settings. They retain the connection's existing ClickHouse permissions; this does not grant additional privileges. Endpoint SQL remains SELECT-only, and endpoint and Ecto-generated read paths retain policy enforcement.

Historical endpoint versions retain current endpoint-owned limits. Cached snapshot reloads also overlay current endpoint policy. Saving endpoint-owned limits invalidates caches for all endpoint versions and stops their active refresh tasks. Already-running foreground queries finish for their existing callers; new callers use replacement caches with current settings.

Successful result caches remain class-agnostic: changing the class does not create a different result-cache key or run a query on a cache hit. Misses and background refreshes enforce policy at execution time. A refresh retains the class from the request that originally created the cache and uses backend policy obtained through the metadata cache, subject to the consistency limitations below. Backend class-policy changes invalidate backend metadata across the cluster, but do not evict reusable successful results, restart ingesters, or tear down active read connections.

## Lower-only settings in endpoint SQL

Endpoint queries, including test runs in the endpoint editor, can request tighter limits directly in the outer `SETTINGS` clause without permission to change administrator-enforced policy:

```sql
SELECT count() AS events
FROM my_source
SETTINGS max_execution_time = 2.5, max_threads = 2, priority = 10
```

Ad hoc queries, including `/api/query` and `/api/query/parse`, do not support these requests.

The supported keys are `max_execution_time`, `max_memory_usage`, `max_rows_to_read`, `max_bytes_to_read`, `max_threads`, and `priority`. Requests must be positive numeric literals; only execution time accepts fractions. The execution time must be at least one microsecond and fit in ClickHouse's signed 64-bit microsecond range. Zero, negative values, strings, parameter placeholders, expressions, and case-insensitive duplicate request keys are rejected. Existing allowlisted optimizer settings can remain in the same clause.

At execution time, Logflare first resolves the backend default, original requested class, and administrator-enforced endpoint policy. SQL requests are applied last:

- Resource limits use `min(inherited, requested)`.
- Priority uses `max(inherited, requested)`: a larger positive number means lower urgency. Requesting priority 1 cannot elevate inherited priority 10.
- A requested key without an explicit inherited Logflare policy value fails the query. An absent value does not prove that ClickHouse's profile default is unlimited. Configure the baseline through backend class policy or administrator-enforced endpoint policy first.

Requests above resource ceilings, or requesting higher urgency, are clamped rather than granting additional resources. Class/default priority precedence and administrator policy remain unchanged. Baselines are resolved for each execution, not frozen when SQL is saved. The requested-class policy also remains in force when routing falls back to another physical cluster; use that cluster's ClickHouse profile constraints for hard ceilings.

Only outer-query requests are supported, including on `EXPLAIN SELECT`. Requests in CTEs or subqueries are rejected. Referencing an endpoint that contains these settings as a subquery is also rejected. Before adding a request to a shared endpoint, check whether other endpoints use it as a subquery. An endpoint cannot be saved with sandboxing enabled and lower-only requests in its base SQL. Sandboxed consumer SQL also cannot make these requests. Remove the lower-only requests to keep sandboxing enabled. Ordinary sandboxed queries and optimizer settings are unaffected.

These requests are part of saved SQL and its versions. Restoring an older SQL version restores its requested values, while current administrator-enforced policy still applies. Result-cache hits do not execute SQL. If a cold-cache execution rejects a lower-only request because its class lacks a baseline, callers waiting under a different class can retry through the same cache with a bounded retry budget. A successful retry fills the shared result cache. Background refreshes use the class that created the current cache. Debug logs record requested/effective SQL settings; `system.query_log.Settings` can verify actual server settings when query-setting logging is enabled. Copying the SQL directly to ClickHouse does not apply Logflare's lower-only merge.

Deploy support to every Logflare instance before enabling these SQL requests. They do not add public policy fields, require a migration, or need a separate editor control.

## Policy update consistency

Backend query-class policy updates are eventually consistent across Logflare instances. A successful save confirms that the configuration was persisted, but does not guarantee that every subsequent query immediately uses it. Query metadata fills read the primary database, and invalidation fences in-flight fills. Saves invalidate local query metadata after commit and multicast evictions to peer nodes; WAL delivery provides additional convergence. Multicast delivery and WAL processing are asynchronous, so other instances can temporarily use the previous policy after limits are tightened, relaxed, or removed.

Do not rely on these updates for immediate emergency throttling or hard resource guarantees. Use ClickHouse query-user profile constraints for hard ceilings. During rollout or rollback, verify effective settings using `system.query_log.Settings` across the relevant workloads and read clusters. Cache expiry is not a guaranteed propagation deadline. This limitation concerns backend class-policy updates, not the endpoint-owned limit invalidation described above.

Endpoint-owned policy misses share a fill per endpoint, with primary reads outside the shared cache transaction queue. Invalidation advances a generation and evicts the entry without waiting for primary reads. Superseded fills retry instead of republishing old settings.

If an endpoint-owned policy is not cached and its primary read fails, or a current policy cannot be established within three attempts, execution fails closed with a retryable HTTP 503. Editor Run displays retry guidance instead of disconnecting. Already-cached policies remain usable. During rolling deploys, eviction skips nodes predating the endpoint-policy invalidation API because they cannot hold those cache entries; this does not make it safe to enable policy before all instances support enforcement.

## Deployment and verification

1. Using the backend's actual query credentials, test each configured setting with a small SELECT. Confirm intended values are accepted and values outside profile constraints are rejected. Keep query-user permissions restricted; do not weaken `readonly` or grant DDL to enable policy. See [ClickHouse setting constraints](https://clickhouse.com/docs/operations/settings/constraints-on-settings).
2. Deploy support on every Logflare instance and drain older versions before enabling policy. Coordinate any ClickHouse profile changes with the backend policy; do not change profile defaults first. Use profile constraints for hard ceilings and consider `priority MIN 1` to prevent queries from opting out of prioritization.
3. Enable query-setting logging and inspect `system.query_log.Settings` using query IDs. Verify priorities and limits for configured classes, missing or unknown labels, and requests routed to the default cluster.
4. Load-test competing workloads in staging, then monitor latency, timeouts, query errors, memory, and connections during rollout. Lower-priority queries can retain resources while paused; priority does not provide tenant isolation or aggregate throttling.

To roll back class policy, pass `%{}` to `Logflare.Backends.configure_query_class_settings/3`. Endpoint-owned limits remain active. Coordinate any profile rollback with requests and instances still using class policy.
