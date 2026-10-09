---
name: ingest-qa
description: Run end-to-end ingestion QA against a live local Logflare server. Use when a change touches ingest (HTTP, WebSocket, gRPC), source lookup, SourceSup, rules or rule dispatch, or when asked to verify ingestion works on a running server. Ingests over every channel, verifies source-rule and backend-rule routing through `iex --remsh`, and screenshots the ingested events in the search UI.
---

# Ingest QA

Runs Logflare in single-tenant Postgres mode (no BigQuery or GCP needed), sends events over
every ingest channel, and checks that each routing target stored exactly the events its rule
matches. Most checks are headless and run inside the live node through `iex --remsh`. One check
uses the screenshot harness to show the events in the search UI.

This skill shares its runtime with the `ui-qa` skill:

| Shared piece | Path | Purpose |
|---|---|---|
| Server | `scripts/qa/server.sh` | Dev server in single-tenant Postgres mode as a named node |
| Remote shell | `scripts/qa/remsh.sh` | Runs an `.exs` file inside that node |
| Settings | `scripts/qa/env.sh` | Ports, node name, cookie, public token, backend URL |
| Screenshots | `scripts/screenshot/` | Playwright harness; this skill adds `specs/ingest-search.spec.ts` |

## What it covers

- **Channels**: HTTP by source token, HTTP by source name, the `/logs` WebSocket `LogChannel`,
  and gRPC OTLP logs export. HTTP auth is checked to reject unknown sources and missing or
  wrong API keys with 401.
- **Rule dispatch**: one source `qa_ingest_main` with four rules, defined in
  `scripts/targets.exs`:

  | Target | Type | LQL | Receives |
  |---|---|---|---|
  | `qa_ingest_sink` | source rule (sink) | `error` | error |
  | `qa_ingest_drain_a` | backend rule (Postgres) | `error` | error |
  | `qa_ingest_drain_b` | backend rule (Postgres) | `warn` | warn |
  | `qa_ingest_drain_c` | backend rule (Postgres) | `~"error\|warn"` | error, warn |

  Each run sends an `error`, a `warn` and an `info` event per channel, tagged with a run id.
  Verification compares each target's stored messages for that run id against the expected
  set, so it catches both missing and wrongly routed events. `info` must reach only the main
  source.
- **Node state**: SourceSup is running for the main and sink sources, and a rule child is
  running for each backend rule.
- **Search UI**: `specs/ingest-search.spec.ts` opens the main source's search page filtered by
  the run id, asserts that exactly this run's events are listed, and captures the query bar
  and the results list.

## Steps

1. Start Postgres. A container restart stops it, so check `pg_isready` first.
2. Install the UI and harness dependencies once:

   ```bash
   npm --prefix assets ci
   npm --prefix scripts/screenshot ci
   ```

   Without the assets the dev server serves an unstyled UI and the screenshot spec fails.
3. Start the server in the background and keep its log:

   ```bash
   scripts/qa/server.sh > /tmp/logflare-qa-server.log 2>&1 &
   ```

   The first run compiles the dev build.
4. Run the QA. It waits for `/health`, so it can start straight away:

   ```bash
   .claude/skills/ingest-qa/scripts/run.sh
   ```

   It prints `QA_CHECK PASS|FAIL` lines, `QA_DIFF` lines with missing and unexpected events,
   then `QA_RUN <run id> PASS|FAIL`, and exits non-zero on failure. Setup is idempotent, so
   rerun freely; each run uses a new run id. Set `QA_SKIP_SCREENSHOT=1` to skip the UI check.
5. Verify the screenshots yourself, as in step C3 of the `ui-qa` skill: read each
   `scripts/screenshot/.generated/ingest-search-*.json` and its PNG, and confirm or refute
   each expectation. A passing spec only proves the DOM state.
6. Check the server log for errors raised during the run. `inotify-tools` errors only affect
   live reload.
7. Stop the server with `kill` on its PID. Do not halt it from a remote shell.

## Extending

- **More backends or rules**: add an entry to `targets` in `scripts/targets.exs`. Setup creates
  the backend or sink source and its rule, and verify picks it up.
- **Change-specific checks**: put them in a new `.exs` script and run it with
  `scripts/qa/remsh.sh <script.exs> KEY=VALUE`. `{{KEY}}` placeholders are replaced, and
  `{{SCRIPT_DIR}}` is set to the script's directory.
- **More UI checks**: extend `scripts/screenshot/specs/ingest-search.spec.ts`, following the
  spec rules in the `ui-qa` skill.

## Pitfalls

- **Ending a piped `iex --remsh` session with EOF stops the remote node.** `remsh.sh` avoids
  this by halting the local probe node at the end of each script. Never pipe into
  `iex --remsh` directly, and never call `System.halt/0` in a remote session: it runs on the
  server.
- **Do not switch `QA_PUBLIC_TOKEN` back to an earlier value on the same dev database.** Each
  boot revokes the previous public token, and a later boot with the old value finds the
  revoked row and does not restore it. HTTP ingest still passes, because the default user's
  legacy API key is the first token ever used, but the WebSocket and gRPC checks fail with 403.
- **The single-tenant sign-in redirect drops the query string.** The search spec visits
  `/dashboard` first to sign in, then opens the search URL.
- `Rules.create_rule/2` fails on a source that has no schema yet. Setup parses the LQL against
  `SchemaBuilder.initial_table_schema/0` and uses `Rules.create_rule/1` instead.
- Load backends with `Backends.get_backend/1`. `Repo.get_by(Backend, ...)` leaves `config` nil.
- The server node runs only the gRPC server. `grpc.exs` starts a `GRPC.Client.Supervisor` for
  the duration of the export.
- Context caches are busted through Postgres logical replication. Without `wal_level=logical`,
  rule changes are not seen until the cache expires, so setup clears the rules and backends
  caches after changing rules.
