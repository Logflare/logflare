---
name: ingest-qa
description: Run end-to-end ingestion QA against a live local Logflare server with `mix qa.ingest`. Use when a change touches ingest (HTTP, WebSocket, gRPC), source lookup, SourceSup, rules or rule dispatch, or when asked to verify ingestion on a running server.
---

# Ingest QA

`mix qa.ingest` sends a tagged run of events over HTTP (by source token and by name), the
`/logs` WebSocket and gRPC OTLP. It then checks inside the server node that each routing
target stored exactly the events its rule matches, and captures the run in the search UI.
It shares the server, browser and report code in `test/support/qa/` with the `ui-qa` skill.

The targets in `Logflare.QA.Ingest.Targets` hang off one source, `qa_ingest_main`, which must
store every event:

| Target | Rule | LQL | Stores |
|---|---|---|---|
| `qa_ingest_sink` | source | `error` | error |
| `qa_ingest_drain_a` | Postgres backend | `error` | error |
| `qa_ingest_drain_b` | Postgres backend | `warn` | warn |
| `qa_ingest_drain_c` | Postgres backend | `~"error\|warn"` | error, warn |

Add a tuple there to route to another backend.

## Steps

1. Set up and start the server as in the `ui-qa` skill: `mix deps.get`, `npm --prefix assets ci`
   and `mix qa.server`.
2. Run `mix qa.ingest`. It prints `QA_CHECK` lines, with missing and unexpected events for a
   routing failure, and exits non-zero on any failure. Rerun freely: setup is idempotent and
   each run has its own id. `--no-screenshot` skips the UI check.
3. Verify `tmp/qa/ingest-search-results.png` against its `.json` as in step C2 of `ui-qa`.
4. Check the server log for errors raised during the run.

To add a server-side check, return data from a function under `test/support/qa/ingest/`, call
it with `Logflare.QA.Remote.call/3`, and print it with `Logflare.QA.Report.check/3`.
`Remote.connect!/0` loads the current QA modules onto the server, so no restart is needed.

## Pitfalls

- Code run with `Remote.call/3` must return its result: its output does not reach your terminal.
- To poke the server by hand, use `iex --sname probe --cookie logflare_qa --remsh logflare_qa@<host>`
  interactively. Piping a script into it and ending with EOF stops the server.
- Do not switch `QA_PUBLIC_TOKEN` back to a value this dev database used before: boot leaves
  it revoked, and the WebSocket and gRPC checks fail with 403.
- The single-tenant sign-in redirect drops the query string, so the search spec signs in at
  `/dashboard` first.
