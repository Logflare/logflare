# Logflare

Logflare is a real-time log aggregation platform built with Elixir/Phoenix. The codebase follows Phoenix context patterns. Setup and local-environment details live in `DEVELOPMENT.md`.

## Where Things Live

- **Backend adaptors**: `lib/logflare/backends/adaptor/` (note: "adaptor" spelling). Each implements the `Logflare.Backends.Adaptor` behaviour; review it for required and optional callbacks.
- **Log ingestion**: `lib/logflare/logs/` is a namespace, not a single context module (`Logflare.Logs.Processor`, `Logflare.Logs.Otel*`, source parsers, validators).
- **Event type detection**: `Logflare.LogEvent.TypeDetection` (`lib/logflare/logs/log_event/type_detection.ex`)
- **SQL parsing**: `lib/logflare/sql.ex`, `lib/logflare/sql/`
- **LQL parsing**: `lib/logflare/lql.ex`, `lib/logflare/lql/`
- **Mapper**: `lib/logflare/mapper/`
- **Rust NIFs**: `native/sqlparser_ex`, `native/mapper_ex`, `native/arrowipc_ex`
- **Custom Ecto types**: `lib/logflare/ecto/` - review existing types before creating new ones
- **Custom credo checks**: `lints/credo/`
- **Test support**: `test/support/factory.ex` (ExMachina factories), `test/test_helper.exs` (Mimic setup)
- **Public docs**: `docs/docs.logflare.com/docs`

## Development Commands

```bash
make start                      # Start dev server (port 4000)

mix ecto.setup                  # Create and migrate local database
mix ecto.reset                  # Drop and recreate local dev database
MIX_ENV=test mix ecto.reset     # Reset local test database

# Testing (full suite takes ~15 minutes - avoid unless asked)
mix test path/to/test.exs       # Run specific test file (creates and migrates the test DB first)
mix test path/to/test.exs:42    # Run test at specific line
mix test.only path/to/test.exs  # Skip DB create/migrate when the test DB is already current
mix test --only focus           # Run tests tagged with @tag :focus

mix format                      # Format code
mix lint.all                    # credo --strict plus migration checks
MIX_ENV=test mix ci             # Run every gate CI enforces
```

`mix ci` must run under `MIX_ENV=test`. The structure baseline is scoped to the test env, so a plain `mix ci` aborts on a scope mismatch. Check the exit code rather than grepping output.

## Static Analysis and CI Gates

Before reading files one by one, use these tools. They answer structural questions faster than grep. All support `--format json`.

- `mix reach.map` - project map: modules, coupling, `--hotspots`, `--boundaries`, `--effects`, `--depth`
- `mix reach.inspect <Mod.fun/arity | file:line> --impact --deps` - blast radius before editing a function
- `mix reach.trace --from params --to write!` - data flow for security and refactor reasoning
- `mix reach.otp` - GenServer state machines, missing message handlers, supervision trees
- `mix ex_dna` lists duplicated code; `mix ex_dna.explain <n>` suggests the extraction

`mix ci` runs `test.compile`, `test.format`, `lint.all`, `test.security`, `test.slop`, and `test.structure` (see the `ci` alias in `mix.exs`). Do not disable a gate to go green - fix the code. CI also runs `mix test.typings` (dialyzer) as a separate job.

- **`lint.all`**: credo with the **ex_slop** plugin (LLM patterns such as blanket rescues, narrator comments, anti-idiomatic `Enum`). Checks with pre-existing findings sit in `ex_slop_backlog` in `config/.credo.exs`. Fixing a backlog check's findings and removing it from the list is welcome. Never add a check to the list to go green.
- **`test.slop`**: `ex_dna --max-clones N`, with `N` set in `mix.exs`. The number only goes down. If you remove clones, lower it. Never raise it.
- **`test.structure`**: `reach.check --smells` against `.reach.baseline.json`, so only new smells fail. To accept a new smell, run `mix reach.check --smells --write-baseline .reach.baseline.json`, then `jq . .reach.baseline.json > tmp && mv tmp .reach.baseline.json` so the checked-in file stays pretty-printed.
- **`test.deps`**: `hex.audit` and `deps.audit`. This is advisory in CI, not a gate.

Credo config lives in **`config/.credo.exs`**, not the repo root. `mix credo gen.config` writes a root `.credo.exs` that credo ignores.

## Workflow

- **Never** run the full `mix test` suite unless explicitly asked
- **Never** run code coverage reports - ask the user to run these instead
- **Always** run the entire test file before considering work complete - single-line runs are fine during iteration, but the full file must pass so setup and teardown are exercised
- **Always** run `mix format` and `mix lint.all` after Elixir changes
- **Always** check dependency versions in `mix.exs`, `mix.lock`, and `native/*/Cargo.toml` before making assumptions
- **Always** ask before making refactoring changes beyond the immediate task
- Bug fixes need a regression test that fails on the old code, plus a test pinning adjacent behaviour that must keep working
- Commit messages use conventional prefixes: `fix:`, `feat:`, `chore:`, `refactor:`, optionally scoped (`fix(otel): ...`)
- Update `docs/docs.logflare.com/docs` when a change alters user-visible behaviour, configuration, or API

## Code Style

Several of these rules are not enforced by credo (`MultiAlias`, `AliasOrder`, `StrictModuleLayout`, and `Specs` are disabled), so follow them by hand.

**Module organization** (in order, with blank lines between groups):
1. `@moduledoc`
2. `@behaviour`
3. `use`
4. `import`
5. `require`
6. `alias` (individual lines, alphabetically sorted - never `alias Foo.{Bar, Baz}`)
7. `@module_attribute`
8. `defstruct`
9. `@type`
10. `@callback`, `@macrocallback`, `@optional_callbacks`
11. `defmacro`, `defguard`
12. `def`

**General style**:
- Typespecs for new functions; prefer typespecs over verbose docs
- No inline comments and no explanatory comments above function definitions; rely on clear names and typespecs
- `alias` goes at the top of the module only - never inside `describe`, `setup`, or `test` blocks
- Prefer multiple function heads with pattern matching over `if`/`else` on an argument's value
- Name keyword-list arguments `opts`; qualify derived ones (`insert_opts`) instead of shadowing
- No `@moduledoc` (or `@moduledoc false`) in test files
- Predicate functions end with `?` (e.g., `valid?/1`); reserve the `is_` prefix for guards
- Prefer `Logflare.Utils.Guards` (`is_non_empty_binary`, `is_pos_integer`, `is_atom_value`, `is_event_type`, ...) over hand-written guard expressions
- Never put more than one module in a file
- Avoid `String.to_atom/1` on user input
- Name OTP primitives: `{DynamicSupervisor, name: Logflare.MySup}`

**Block rebinding**: capture the result of `if`/`case`/`cond`; assignments inside the block are lost:
```elixir
socket = if connected?(socket), do: assign(socket, :val, val), else: socket
```

## Project-Specific Patterns

### Web Layer

Use the `LogflareWeb` macros (`use LogflareWeb, :controller`, `:live_view`, `:live_component`, `:html`, ...). They bundle the standard imports and aliases - don't import Phoenix modules by hand.

### Testing

- Mock with `Mimic`. A module must be listed with `Mimic.copy/1` in `test/test_helper.exs` before it can be stubbed.
- ClickHouse adaptor tests need a local ClickHouse on port 8123 (`docker compose up clickhouse`). Without it they hang silently instead of failing.

### Ecto and Migrations

- Embedded schemas use `typed_ecto_schema` (`typed_embedded_schema do ... end`).
- Migrations are replicated to pglogical subscribers. Raw `execute/1` DDL and `Oban.Migration` calls must be wrapped in `Migrator.with_replicated_execute/1`; the custom checks in `lints/credo/` enforce this via `mix lint.all`.
- Every migration needs a working `down`. CI migrates, rolls back, and runs tests (`.github/workflows/elixir-migration-check.yml`).

### Architecture

**Single-tenant mode**: Check `Logflare.SingleTenant.single_tenant?()` - behaviour differs between multi-tenant (SaaS) and single-tenant deployments.

**Log processing**: Broadway pipelines handle ingestion, and `Logflare.Backends.DynamicPipeline` scales them on queue depth. `Logflare.LogEvent.TypeDetection` classifies events as `:log`, `:metric`, or `:trace`, which drives routing and table selection in adaptors. OpenTelemetry protobuf payloads become `LogEvent` structs via the `Logflare.Logs.Otel*` modules.

**ClickHouse pipeline**: One Broadway pipeline per backend. Messages are batched by `{event_type, day_bucket}` via `put_batch_key`, routing to the type-specific OTEL tables (`otel_logs_*`, `otel_metrics_*`, `otel_traces_*`). See `lib/logflare/backends/adaptor/clickhouse_adaptor/`.

**Mapper** (ClickHouse only): Config-driven transformation through the `native/mapper_ex` NIF, with the Elixir interface in `lib/logflare/mapper/native.ex`.
- The per-event mapping path runs on every ingested event and must never return an error. Clamp or default bad values per event, and surface errors only from `compile_mapping`.
- Editing `for_log/0`, `for_metric/0`, or `for_trace/0` in `lib/logflare/mapper/otel_defaults.ex` requires bumping that type's `@*_config_id`. Never bump the IDs of unchanged types.

**SQL parsing**: Rust NIF in `native/sqlparser_ex` using the [`sqlparser`](https://crates.io/crates/sqlparser) crate. The Elixir interface is `lib/logflare/sql/parser.ex`, with higher-level functions in `lib/logflare/sql.ex`.
