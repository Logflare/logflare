# Logflare

Logflare is a real-time log aggregation platform built with Elixir/Phoenix. The codebase follows Phoenix context patterns with domain-driven design.

## Core Contexts

- **`Logflare.Logs`** - Central log processing, validation, and routing
- **`Logflare.Sources`** - Log source management and schema handling
- **`Logflare.Backends`** - Pluggable backend adapters (BigQuery, ClickHouse, PostgreSQL, Elasticsearch, etc.)
- **`Logflare.Endpoints`** - Parameterized SQL endpoints for analytics
- **`Logflare.Users`** - User management and authentication
- **`Logflare.Teams`** - Multi-tenant team support
- **`Logflare.Billing`** - Subscription and usage tracking with Stripe
- **`Logflare.Alerting`** - Log-based alerting system

## Backend Adapters

The application uses an adapter pattern for different backends:
- BigQuery (primary analytics backend)
- ClickHouse (high-performance analytics)
- PostgreSQL (relational data)
- Elasticsearch (search and indexing)
- Datadog, Loki, S3 (external integrations)
- Slack, Webhooks (notifications)

## Important File Locations

- **Main Application**: `lib/logflare.ex`, `lib/logflare/application.ex`
- **Web Layer**: `lib/logflare_web/` (controllers, live views, channels)
- **Core Contexts**: `lib/logflare/` (domain logic organized by context)
- **Backend Adapters**: `lib/logflare/backends/adaptor/`
- **SQL Parsing**: `lib/logflare/sql.ex`, `lib/logflare/sql/`
- **LQL Parsing**: `lib/logflare/lql.ex`, `lib/logflare/lql/`
- **Database**: `priv/repo/migrations/`, `priv/repo/seeds.exs`
- **Configuration**: `config/` directory
- **Tests**: `test/` directory
- **Public-Facing Documentation**: `docs/docs.logflare.com/docs` directory

## SQL Parsing

- SQL parsing is handled via a rust-based NIF that you can find in `native/sqlparser_ex`. This NIF relies on the [`sqlparser`](https://crates.io/crates/sqlparser) crate and produces AST for common dialects. The elixir side of the project leverages this AST within `lib/logflare/sql.ex`. The interface to that NIF can be found in `lib/logflare/sql/parser.ex`.
- Be sure to check the version of the `sqlparser` crate in `native/sqlparser_ex/Cargo.toml` before making any assumptions.

## Common Development Commands
- `make start` - Starts the application in development mode (port 4000)
- `mix test` - Runs the full test suite (_Do not do this as it takes a long time_)
- `mix format` - Formats the code
- `mix credo` - Checks the code for style and correctness
- `mix ecto.setup` - create and migrate the local database
- `mix ecto.reset` - drop and recreate the local dev database
- `MIX_ENV=test mix ecto.reset` - reset the local test database

## General Elixir Guidelines
- Elixir lists **do not support index based access via the access syntax**

  **Never do this (invalid)**:

      i = 0
      mylist = ["blue", "green"]
      mylist[i]

  Instead, **always** use `Enum.at`, pattern matching, or `List` for index based list access, ie:

      i = 0
      mylist = ["blue", "green"]
      Enum.at(mylist, i)

- Elixir variables are immutable, but can be rebound, so for block expressions like `if`, `case`, `cond`, etc
  you *must* bind the result of the expression to a variable if you want to use it and you CANNOT rebind the result inside the expression, ie:

      # INVALID: we are rebinding inside the `if` and the result never gets assigned
      if connected?(socket) do
        socket = assign(socket, :val, val)
      end

      # VALID: we rebind the result of the `if` to a new variable
      socket =
        if connected?(socket) do
          assign(socket, :val, val)
        end

- **Never** nest multiple modules in the same file as it can cause cyclic dependencies and compilation errors
- **Never** use map access syntax (`changeset[:field]`) on structs as they do not implement the Access behaviour by default. For regular structs, you **must** access the fields directly, such as `my_struct.field` or use higher level APIs that are available on the struct if they exist, `Ecto.Changeset.get_field/2` for changesets
- Elixir's standard library has everything necessary for date and time manipulation. Familiarize yourself with the common `Time`, `Date`, `DateTime`, and `Calendar` interfaces by accessing their documentation as necessary.
- Avoid `String.to_atom/1` on user input (memory leak risk)
- Predicate function names should not start with `is_` and should end in a question mark. Names like `is_thing` should be reserved for guards
- Elixir's builtin OTP primitives like `DynamicSupervisor` and `Registry`, require names in the child spec, such as `{DynamicSupervisor, name: Logflare.MyDynamicSup}`, then you can use `DynamicSupervisor.start_child(Logflare.MyDynamicSup, child_spec)`

## Development Principles
- Ensure you check the version of dependencies in `mix.exs` (and `mix.lock`) before making assumptions.
- If you see refactoring opportunities, please ask before making changes.
- Various helpers and utility modules can be found in `lib/logflare/utils`.
- `lib/logflare/utils.ex` contains some additional utility functions.
- Leverage `Logflare.Utils.Guards` when it makes sense to do so in new modules.
- This project uses `Mimic` for mocking and stubbing. See `test/test_helper.exs` for the setup of those.
- You can find the data factories for testing in `test/support/factory.ex`
- **Never** run the full `mix test` suite unless told to as it takes about 15 minutes to run.
- **Never** run a code coverage report as it requires the full test suite to run. Instead, ask me to run coverage or the full test suite.

## Code Style
- Always order module statements in this sequence: `@moduledoc`, `@behavior`, `use`, `import`, `require`, `alias`, `@module_attribute`, `defstruct`, `@type`, `@callback`, `@macrocallback`, `@optional_callbacks`, `defmacro`, `defguard`, `def`, etc.
- Add a blank line between each grouping, and sort the terms (like alias names) alphabetically.
- **Never** use multiple aliases in one line like `alias Logflare.Lql.{Validator, FilterRule, ChartRule}` and instead use individual aliases
- Create typespecs when writing new functions.
- Avoid in-line comments when writing code and instead rely on good typespecs and clear logic.
- Do not add module docs for ExUnit test files. This also means to avoid `@moduledoc false` in test files.
- Leverage good typespecs over verbose module and function docs. No need to explain every argument and return value in docs.
- When in doubt, reference the https://github.com/christopheradams/elixir_style_guide for guidance.
