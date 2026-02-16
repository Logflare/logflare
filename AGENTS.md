# Logflare

Logflare is a real-time log aggregation platform built with Elixir/Phoenix. The codebase follows Phoenix context patterns with domain-driven design.

## Core Contexts

- **`Logflare.Logs`** - Central log processing, validation, and routing
- **`Logflare.Sources`** - Log source management and schema handling
- **`Logflare.Backends`** - Pluggable backend adapters (BigQuery, ClickHouse, PostgreSQL, and others — see `lib/logflare/backends/adaptor/` for full list)
- **`Logflare.Endpoints`** - Parameterized SQL endpoints for analytics
- **`Logflare.Users`** - User management and authentication
- **`Logflare.Teams`** - Multi-tenant team support
- **`Logflare.Billing`** - Subscription and usage tracking with Stripe
- **`Logflare.Alerting`** - Log-based alerting system
- **`Logflare.Mapper`** - Data mapping/transformation system with Rust NIF backend
- **`Logflare.Rules`** - Rule engine for log routing
- **`Logflare.SourceSchemas`** - Source schema management
- **`Logflare.Partners`** - Partner integrations (Vercel, etc.)

## Important File Locations

- **Backend Adapters**: `lib/logflare/backends/adaptor/` (note: "adaptor" spelling)
- **SQL Parsing**: `lib/logflare/sql.ex`, `lib/logflare/sql/`
- **LQL Parsing**: `lib/logflare/lql.ex`, `lib/logflare/lql/`
- **Mapper**: `lib/logflare/mapper/`, `native/mapper_ex/`
- **Test Support**: `test/support/factory.ex` (data factories), `test/test_helper.exs` (Mimic setup)
- **Public-Facing Documentation**: `docs/docs.logflare.com/docs` directory

## SQL Parsing

SQL parsing is handled via a Rust NIF in `native/sqlparser_ex`, using the [`sqlparser`](https://crates.io/crates/sqlparser) crate. The Elixir interface is in `lib/logflare/sql/parser.ex` with higher-level functions in `lib/logflare/sql.ex`. Check `native/sqlparser_ex/Cargo.toml` for the crate version before making assumptions.

## Configuration-Based Data Mapping

Config-based data mapping/transformation (currently for ClickHouse backends only) is handled via a Rust NIF in `native/mapper_ex/`. The Elixir interface is in `lib/logflare/mapper/native.ex` with higher-level functions in `lib/logflare/mapper/`.

## Development Commands

```bash
# Application
make start                      # Start dev server (port 4000)

# Database
mix ecto.setup                  # Create and migrate local database
mix ecto.reset                  # Drop and recreate local dev database
MIX_ENV=test mix ecto.reset     # Reset local test database

# Testing (full suite takes ~15 minutes - avoid unless asked)
mix test path/to/test.exs       # Run specific test file
mix test path/to/test.exs:42    # Run test at specific line
mix test --only focus           # Run tests tagged with @tag :focus

# Code Quality
mix format                      # Format code
mix credo                       # Check style and correctness
```

## Workflow

- **Never** run the full `mix test` suite unless explicitly asked - it takes ~15 minutes
- **Never** run code coverage reports - ask the user to run these instead
- **Always** run the entire test file (`mix test path/to/test.exs`) before considering work complete - single line tests are fine during iteration, but the full file must pass to ensure proper test setup and teardown
- **Always** check dependency versions in `mix.exs` and `mix.lock` before making assumptions
- **Always** ask before making refactoring changes beyond the immediate task
- This project uses `Mimic` for mocking/stubbing
- Leverage `Logflare.Utils.Guards` in new modules when appropriate

## Code Style

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
- Create typespecs for new functions; prefer typespecs over verbose docs
- Avoid inline comments; rely on clear logic and typespecs
- No `@moduledoc` (or `@moduledoc false`) in test files
- Predicate functions end with `?` (e.g., `valid?/1`); reserve `is_` prefix for guards

## Elixir Pitfalls

These are common mistakes - avoid them:

**List access**: Lists don't support bracket access. Use `Enum.at/2`:
```elixir
# Wrong: mylist[0]
# Right: Enum.at(mylist, 0)
```

**Block rebinding**: Must capture the result of `if`/`case`/`cond`:
```elixir
# Wrong - assignment inside block is lost:
if connected?(socket), do: socket = assign(socket, :val, val)

# Right - capture the block result:
socket = if connected?(socket), do: assign(socket, :val, val), else: socket
```

**Struct access**: Structs don't implement Access. Use dot notation or specific APIs:
```elixir
# Wrong: changeset[:field]
# Right: changeset.field or Ecto.Changeset.get_field(changeset, :field)
```

**Additional guidance**:
- Never nest multiple modules in the same file (causes cyclic dependencies)
- Avoid `String.to_atom/1` on user input (memory leak risk)
- OTP primitives need names: `{DynamicSupervisor, name: Logflare.MySup}`
- Use standard library for date/time (`DateTime`, `Date`, `Time`, `Calendar`)

## Project-Specific Patterns

### Web Layer

Use the `LogflareWeb` macro system for controllers, views, and LiveViews:
```elixir
use LogflareWeb, :controller
use LogflareWeb, :live_view
use LogflareWeb, :component
```
These bundle standard imports, aliases, and helpers - don't manually import Phoenix modules.

### Backend Adaptors

Backend adaptors implement the `Logflare.Backends.Adaptor` behaviour. Review that module for required and optional callbacks. See existing adaptors in `lib/logflare/backends/adaptor/` for implementation patterns.

### Testing

**ExMachina factories** for test data (defined in `test/support/factory.ex`):
```elixir
insert(:user)                    # Insert into database
insert(:source, user: user)      # With associations
build(:backend)                  # Build without inserting
```

### Ecto Patterns

Custom Ecto types are defined in `lib/logflare/ecto/` — review existing types before creating new ones.

**Typed schemas**: This project uses `typed_ecto_schema` for type-safe embedded schemas:
```elixir
typed_embedded_schema do
  field(:name, :string)
end
```

### Architecture

**Single-tenant mode**: Check `Logflare.SingleTenant.single_tenant?()` - behavior differs between multi-tenant (SaaS) and single-tenant deployments.

**Log processing**: Broadway pipelines handle log ingestion. `Logflare.Backends.DynamicPipeline` scales pipelines dynamically based on load. Incoming events are classified as `:log`, `:metric`, or `:trace` by `Logflare.Logs.LogEvent.TypeDetection`, which determines routing and table selection in backend adaptors. OpenTelemetry protobuf payloads are converted into `LogEvent` structs via modules in `lib/logflare/logs/`.

**ClickHouse consolidated pipeline**: The ClickHouse adaptor uses a single Broadway pipeline per backend. Messages are partitioned by `log_type` via `put_batch_key`, routing to type-specific OTEL tables (`otel_logs_*`, `otel_metrics_*`, `otel_traces_*`). See `lib/logflare/backends/adaptor/clickhouse_adaptor/` for internals.
