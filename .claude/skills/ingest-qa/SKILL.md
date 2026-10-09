---
name: ingest-qa
description: Run end-to-end ingestion QA against a live local Logflare server with `mix qa.ingest`. Use when a change touches ingest (HTTP, WebSocket, gRPC), source lookup, SourceSup, rules or rule dispatch, or when asked to verify ingestion works on a running server. Ingests over every channel, verifies source-rule and backend-rule routing inside the server node, and screenshots the ingested events in the search UI.
---

# Ingest QA

`mix qa.ingest` sends events over every ingest channel to a server started with
`mix qa.server`, then checks that each routing target stored exactly the events its rule
matches. Most checks are headless and run inside the server node over distributed Erlang.
One check drives the search UI with the screenshot harness of the `ui-qa` skill.

The code is Elixir in `test/support/qa/`, compiled in the dev and test environments:

| Part | Modules | Runs on |
|---|---|---|
| Shared with `ui-qa` | `Logflare.QA.Config`, `Logflare.QA.Browser`, `Logflare.QA.Report`, `mix qa.server` | client, server |
| Remote calls | `Logflare.QA.Remote` loads the QA modules onto the server node and calls them with `:erpc` | client |
| Ingest | `Logflare.QA.Ingest.Targets`, `.Channels`, `.SearchSpec`, `mix qa.ingest` | client |
| Ingest, server side | `Logflare.QA.Ingest.Setup`, `.Verify` | server node |

## What it covers

- **Channels**: HTTP by source token, HTTP by source name, the `/logs` WebSocket `LogChannel`,
  and gRPC OTLP logs export. HTTP auth is checked to reject unknown sources and missing or
  wrong API keys with 401.
- **Rule dispatch**: one source `qa_ingest_main` with four rules, defined in
  `Logflare.QA.Ingest.Targets`:

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
- **Search UI**: `Logflare.QA.Ingest.SearchSpec` opens the main source's search page filtered
  by the run id, asserts that exactly this run's events are listed, and captures the query bar
  and the results list.

## Steps

1. Start Postgres. A container restart stops it, so check `pg_isready` first.
2. Install the dependencies once: `mix deps.get` and `npm --prefix assets ci`.
   The assets give the UI its styles and hold the Playwright driver.
   In a cloud session, set `PLAYWRIGHT_CHROMIUM_PATH=/opt/pw-browsers/chromium`.
3. Start the server in the background and keep its log:

   ```bash
   mix qa.server > /tmp/logflare-qa-server.log 2>&1 &
   ```

   The first run compiles the dev build.
4. When `curl -fs localhost:4000/health` succeeds, run the QA:

   ```bash
   mix qa.ingest
   ```

   It prints `QA_CHECK PASS|FAIL` lines, with the missing and unexpected events for a failed
   routing check, `QA_CAPTURE` lines, then `QA_RESULT`. It exits non-zero on failure. Setup is
   idempotent, so rerun freely; each run uses a new run id. Pass `--no-screenshot` to skip the
   UI check.
5. Verify the captures yourself, as in step C3 of the `ui-qa` skill: read each
   `tmp/qa/ingest-search-*.json` and its PNG, and confirm or refute each expectation. A passing
   spec only proves the DOM state.
6. Check the server log for errors raised during the run. `inotify-tools` errors only affect
   live reload.
7. Stop the server with `kill` on its PID.

## Extending

- **More backends or rules**: add an entry to `targets` in `Logflare.QA.Ingest.Targets`.
  Setup creates the backend or sink source and its rule, and verification picks it up.
- **Change-specific checks**: add a function to a module under `test/support/qa/` that returns
  data, call it with `Logflare.QA.Remote.call/3` from `mix qa.ingest`, and print the result with
  `Logflare.QA.Report.check/3`. `Remote.connect!/0` loads the current QA modules onto the
  server node, so no restart is needed.
- **More UI checks**: extend `Logflare.QA.Ingest.SearchSpec`, following the spec rules in the
  `ui-qa` skill.

## Pitfalls

- **Remote functions must return data, not print it.** Output from the server node does not
  reach the `mix qa.ingest` terminal.
- **To poke the server by hand, use `iex --sname probe --cookie logflare_qa --remsh logflare_qa@<host>`
  interactively.** Piping a script into `iex --remsh` and ending with EOF stops the server
  node, and `System.halt/0` in a remote shell runs on the server.
- **Do not switch `QA_PUBLIC_TOKEN` back to an earlier value on the same dev database.** Each
  boot revokes the previous public token, and a later boot with the old value finds the
  revoked row and does not restore it. HTTP ingest still passes, because the default user's
  legacy API key is the first token ever used, but the WebSocket and gRPC checks fail with 403.
- **The single-tenant sign-in redirect drops the query string.** The search spec visits
  `/dashboard` first to sign in, then opens the search URL.
- `Rules.create_rule/2` fails on a source that has no schema yet. Setup parses the LQL against
  `SchemaBuilder.initial_table_schema/0` and uses `Rules.create_rule/1` instead.
- Load backends with `Backends.get_backend/1`. `Repo.get_by(Backend, ...)` leaves `config` nil.
- Context caches are busted through Postgres logical replication. Without `wal_level=logical`,
  rule changes are not seen until the cache expires, so setup clears the rules and backends
  caches after changing rules.
