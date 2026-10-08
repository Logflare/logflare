---
name: ingest-qa
description: Run end-to-end ingestion QA against a live local Logflare server. Use when a change touches ingest (HTTP, WebSocket, gRPC), source lookup, SourceSup, rules or rule dispatch, or when asked to verify ingestion works on a running server. Starts the server as a named node, ingests over every channel, and verifies source-rule and backend-rule routing through `iex --remsh`.
---

# Ingest QA

Runs Logflare in single-tenant Postgres mode (no BigQuery or GCP needed), sends events over
every ingest channel, and checks that each routing target stored exactly the events its rule
matches. All checks run inside the live node through `iex --remsh`.

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

## Steps

1. Start Postgres. A container restart stops it, so check `pg_isready` first.
2. Start the server in the background and keep its log:

   ```bash
   .claude/skills/ingest-qa/scripts/server.sh > /tmp/ingest-qa-server.log 2>&1 &
   ```

   This creates the backend database if `psql` is available, migrates the dev database and
   runs `mix phx.server` as node `ingest_qa@<host>`. The first run compiles the dev build.
3. Run the QA. It waits for `/health`, so it can start straight away:

   ```bash
   .claude/skills/ingest-qa/scripts/run.sh
   ```

   It prints `QA_CHECK PASS|FAIL` lines, `QA_DIFF` lines with missing and unexpected events,
   then `QA_RUN <run id> PASS|FAIL`, and exits non-zero on failure. Setup is idempotent, so
   rerun freely; each run uses a new run id.
4. Check the server log for errors raised during the run. Errors from the asset watcher
   (`esbuild`, `watcher_command_error`) and `inotify-tools` only affect local UI builds.
5. Stop the server with `kill` on its PID. Do not halt it from a remote shell.

Settings such as ports, node name, cookie, public token and backend URL live in
`scripts/env.sh` and can be overridden through the environment.

## Extending

- **More backends or rules**: add an entry to `targets` in `scripts/targets.exs`. Setup creates
  the backend or sink source and its rule, and verify picks it up.
- **Change-specific checks**: put them in a new `.exs` script and run it with
  `scripts/remsh.sh <script.exs> KEY=VALUE`. `{{KEY}}` placeholders are replaced, and
  `{{SKILL_DIR}}` is always set.

## Pitfalls

- **Ending a piped `iex --remsh` session with EOF stops the remote node.** `remsh.sh` avoids
  this by halting the local probe node at the end of each script. Never pipe into
  `iex --remsh` directly, and never call `System.halt/0` in a remote session: it runs on the
  server.
- `Rules.create_rule/2` fails on a source that has no schema yet. Setup parses the LQL against
  `SchemaBuilder.initial_table_schema/0` and uses `Rules.create_rule/1` instead.
- Load backends with `Backends.get_backend/1`. `Repo.get_by(Backend, ...)` leaves `config` nil.
- The server node runs only the gRPC server. `grpc.exs` starts a `GRPC.Client.Supervisor` for
  the duration of the export.
- Context caches are busted through Postgres logical replication. Without `wal_level=logical`,
  rule changes are not seen until the cache expires, so setup clears the rules and backends
  caches after changing rules.
