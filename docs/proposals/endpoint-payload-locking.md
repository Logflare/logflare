# Locking Endpoint payload shapes to prevent breaking API changes

Status: investigation / proposal
Related: O11Y-2073, incident follow-up on endpoint query-shape breakage

## Problem

An `Endpoints.EndpointQuery` renders its result rows directly to API
consumers with no shape enforcement:

- `EndpointsController.query/2` (`lib/logflare_web/controllers/endpoints_controller.ex`)
  calls `Endpoints.run_cached_query/3` and renders `result.rows` verbatim
  via `EndpointsView."query.json"` (`lib/logflare_web/views/endpoints_view.ex`),
  i.e. the payload is `%{"result" => rows}` where `rows` is whatever the
  backend adaptor (BigQuery/ClickHouse/Postgres) returns for the query.
- Nothing compares the columns/types of that result against what was
  returned before the query was last edited. A query edit that renames a
  column, changes a type, drops a field, or changes cardinality/nesting
  ships straight to downstream consumers.

During the incident referenced in Slack, endpoint query-shape changes were
a direct cause of breakage, and also part of the mitigation (reverting the
query). This proposal defines a mechanism to catch shape-changing edits
before they are saved.

## Existing building blocks

- **Schema**: `EndpointQuery` (`lib/logflare/endpoints/endpoint_query.ex`)
  already has `sandboxable`, `language`, `query`, and versioning via
  PaperTrail (`@version_snapshot_fields`).
- **Params**: `Sql.parameters/1` (`lib/logflare/sql.ex`) extracts `@param`
  placeholders from the query AST; `Endpoints.run_query/3` uses this to
  bind `declared_params`. The LiveView editor
  (`lib/logflare_web/live/endpoints/endpoints_live.ex`, `assign_updated_params_form/3`)
  already turns these into a form of `%{param_name => value}` — this is
  the natural shape for a stored test case's input.
- **Run-query preview**: the LiveView already has a `"run-query"` event
  handler that executes the query against live data for preview
  (`endpoints_live.ex`, around the `run-query`/`run-sandbox-query` events)
  using `Endpoints.run_query_string/3` with `use_query_cache: false`. This
  is the natural place to also run stored test cases.
- **Schema tracking precedent**: `Logflare.SourceSchemas.SourceSchema`
  already stores a flattened schema map (`schema_flat_map`) per source and
  diffs it as BigQuery table schemas evolve
  (`lib/logflare/source_schemas/source_schema.ex`). Endpoint payload
  locking can reuse the same "flatten a result row into a comparable
  shape" approach rather than inventing a new schema format.
- **Sandboxing**: `sandboxable` queries accept caller-supplied SQL/LQL at
  request time (`Endpoints.run_query/3`, `Sql` sandbox validation in
  `lib/logflare/sql.ex`), so their result shape is inherently
  caller-controlled. Per the requirements, shape-locking does not apply to
  sandboxed queries.

## Proposed design

### 1. Data model: `EndpointQueryTestCase` (new schema/table)

```
endpoint_query_test_cases
  id
  endpoint_query_id  (references endpoint_queries)
  name               (string)
  params             (map)  -- input query params, e.g. %{"user_id" => "123"}
  expected_schema    (map)  -- flattened output-shape schema supplied by the user
  last_run_schema    (map, nullable)  -- flattened schema observed on last run
  last_run_at        (utc_datetime, nullable)
  last_run_status    (enum: :passed | :failed | :error, nullable)
  inserted_at / updated_at
```

- `params` mirrors the `%{param_name => value}` shape already produced by
  `assign_updated_params_form/3`, so the LiveView form that builds a param
  map for a manual test run can be reused verbatim to define a test case.
- `expected_schema` and `last_run_schema` use the same "flattened map"
  representation as `SourceSchema.schema_flat_map` (key path → type),
  rather than a full JSON Schema document, so the comparison logic and
  flattening helper (`Logflare.Utils.Map.flat?/1` and friends) can be
  shared with `Logflare.SourceSchemas`.
- Test cases are owned by an `EndpointQuery` and are only meaningful when
  `sandboxable == false`; the UI/context should reject creating test cases
  for sandboxed endpoints (or simply skip validation for them, per
  requirement 4 below).

### 2. Shape derivation

Add `Logflare.Endpoints.ShapeSchema` (or extend `Logflare.SourceSchemas`
with a shared helper) that:

1. Takes a list of result rows (as already returned by
   `Endpoints.run_query/3`/`run_query_string/3`).
2. Flattens each row's keys to dotted paths and records the Elixir/Ecto
   type at each path (reusing the flattening approach already used for
   `schema_flat_map`).
3. Unions the flattened shapes across all returned rows (to tolerate
   sparse/optional fields) into a single "observed schema" map.

This gives an `expected_schema` / `observed_schema` pair that can be
diffed structurally (added/removed/retyped keys) rather than doing a
strict row-by-row equality check, which would be brittle against live
data.

### 3. Test execution ("Endpoint tests")

Add `Logflare.Endpoints.run_test_case/1`:

1. Loads the `EndpointQuery` and `EndpointQueryTestCase`.
2. Runs the endpoint's query against live data using the test case's
   `params`, via the existing `run_query_string/3`/`run_query/3` path
   (`use_query_cache: false`, since tests must reflect the current query
   definition, not a cached prior result).
3. Derives the observed schema per §2.
4. Compares observed vs. `expected_schema`:
   - Missing keys, changed types, or (optionally, if configured) newly
     added keys are reported as a diff.
5. Persists `last_run_schema`, `last_run_status`, `last_run_at` on the
   test case for visibility, and returns `{:ok, :passed}` or
   `{:error, diff}`.

Run all test cases for an endpoint with `Logflare.Endpoints.run_test_cases/1`
(`Task.async_stream` over the test case list, bounded concurrency, same
pattern as other fan-out call sites in `Logflare.Backends`).

### 4. Gating query updates

In `Logflare.Endpoints.update_query/4` (`lib/logflare/endpoints.ex`),
before committing the update inside the existing `Repo.transact`:

1. If `sandboxable` is (or becomes) `true`, skip test-case validation
   entirely — sandboxed queries are dynamic by design.
2. Otherwise, if the endpoint has one or more test cases, run them against
   the *proposed* new query (not yet persisted) using the same params.
   This requires `run_test_case/1` to accept an in-memory `EndpointQuery`
   struct/changeset rather than only a persisted one, since the update
   hasn't been committed yet.
3. If any test case fails (schema diff is non-empty) or errors, abort the
   transaction and return `{:error, changeset}` with the failing test case
   names and diffs attached as changeset errors, so the LiveView form can
   surface them next to the query editor (same place `validate_query/2`
   errors already surface).
4. If all test cases pass, commit the update and store the new
   `last_run_schema`/`last_run_status` per test case.

This mirrors how `validate_query/2` already blocks invalid SQL from being
saved — test-case failures become another changeset-level validation, just
one that requires executing the query against live data rather than only
parsing it.

### 5. UI

In `EndpointsLive`:

- A "Tests" tab/section per endpoint, listing test cases with name,
  params, expected schema, and last run status/timestamp.
- "Add test case" reuses the existing param-form assignment
  (`assign_updated_params_form/3`) to build `params`, and an explicit
  "Capture current shape as expected" action that runs the query once and
  stores the observed schema as `expected_schema` (so users don't have to
  hand-write a schema).
- "Run tests" button invokes `run_test_cases/1` and displays per-test
  pass/fail with a diff view for failures.
- On "Save query", if any test fails, block the save and show the diff
  inline, consistent with existing `query_error_message` handling for
  `"run-query"`.

### 6. Non-goals / exclusions

- Sandboxed endpoints (`sandboxable == true`) are explicitly excluded from
  shape locking, per requirement — their result shape is caller-defined at
  request time and cannot be pinned to a single expected schema.
- This proposal does not attempt full JSON Schema validation (formats,
  required/optional, enum constraints) — it reuses the existing flattened
  key→type map used by `SourceSchema`, which is sufficient to catch the
  breaking changes seen in the incident (renamed/removed/retyped columns)
  without introducing a new schema DSL.
- Backfilling `expected_schema` for existing endpoints is a one-time
  opt-in action ("capture current shape") rather than automatic, since
  Logflare has no way to know today's shape is actually the desired one.

## Rollout

1. Migration + schema + context functions (`EndpointQueryTestCase`,
   `Endpoints.run_test_case/1`, `run_test_cases/1`).
2. Wire test execution into `update_query/4` behind the test-case-exists
   check (endpoints with zero test cases behave exactly as today — opt-in,
   non-breaking for existing users).
3. LiveView UI for creating/running/viewing test cases.
4. Documentation in `docs/docs.logflare.com/docs` for the new
   endpoint-testing feature.
