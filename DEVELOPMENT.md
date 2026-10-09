# Development

## Dev Env Setup

Pre-requisites:

- asdf (or mise)
- docker

### Setup for Supabase Team

```bash
# install deps
make setup

# decrypt secrets
make decrypt.dev

# start dev server
make start
```

To test the ingestion:

```bash
# Set for testing logging ingestion
sed -i 's/LOGFLARE_LOGGER_BACKEND_API_KEY=.*/LOGFLARE_LOGGER_BACKEND_API_KEY=my-cool-api-key-123/' .dev.env
# Set for testing OTEL ingestion
sed -i 's/LOGFLARE_OTEL_SOURCE_UUID=.*/LOGFLARE_OTEL_SOURCE_UUID=my-otel-source-uuid/' .dev.env
sed -i 's/LOGFLARE_OTEL_ACCESS_TOKEN=.*/LOGFLARE_OTEL_ACCESS_TOKEN=my-cool-api-key-123/' .dev.env

# restart server with iex
make start

# with iex
iex> LogflareLogger.info("testing")
```

To run multi-node cluster:

```bash
# in separate terminals
make start.orange
make start.pink
```

### Setup for External Contributors

```bash
# start local databases
docker-compose up -d db clickhouse telegraf

# install dependencies
make setup

# start in single tenant postgres backend (logflare_dev)
make start.st.pg

# run tests
mix test
mix test.watch
make test.failed
mix test --repeat-until-failure 1000 test/...

# run checks
mix test.coverage
mix test.compile
mix format
mix lint
```

To configure the BigQuery backend, please follow the [BigQuery setup documentation](https://docs.logflare.app/self-hosting/#bigquery-setup).

### Developing for Single Tenant

Use the single tenant `make start.*` variations. This works by switching out the `LOGFLARE_SINGLE_TENANT` env var.

```bash
make start.st.pg
make start.st.bq
```

To develop with Supabase mode:

```bash
make start.sb.bq
make start.sb.pg
```

### Running with Docker Compose

Use any of the variations, which will start logflare in single-tenant mode:

```bash
# to build locally with bq backend
docker compose up db logflare telegraf

# to build locally with pg backend
docker compose -f docker-compose.yml -f docker-compose.pg.yml up db logflare telegraf

# to run latest image locally with bq backend
docker compose -f docker-compose.yml -f docker-compose.latest.yml up db logflare telegraf

# to run latest image locally with pg backend
docker compose -f docker-compose.yml -f docker-compose.latest.yml -f docker-compose.pg.yml up db logflare telegraf
```

### Developing Logflare alongside Supabase CLI

In order to test all changes locally, perform the following steps:

1. Build logflare docker image locally: `docker-compose build`
   - the compose file tags the image locally.
2. CLI repo: run the CLI locally `go run . start`
   - prefix all CLI commands with `go run .`
   - run `go run . init` to create a local Supabase project
3. Update the test Supabase project's config under `supabase/config.toml`
   - Logflare uses the `analytics` namespace.

### Testing Logflare E2E integration with Supabase

These tests live in `test/e2e/supabase` and use Playwright against a local Supabase docker-compose setup.

Run locally:

```bash
cd test/e2e/supabase

# Boot Supabase services
bash ./setup-supabase-services.sh

# Install JS dependencies
npm ci

# Install Playwright browsers and OS deps
npx playwright install --with-deps

# Run all Playwright tests
npm run playwright:test

# Optional: run with Playwright UI
npm run playwright:test:ui
```

For debugging or troubleshooting you can easily interact with the containers with `bin/compose` with any valid
docker-compose commands. You need to be in the same directory.

```bash
cd test/e2e/supabase

# See Logflare container logs
bin/compose logs analytics -f

# Stop all service containers
bin/compose stop

# Remove all containers and associated volumes
bin/compose down -v
```

## Command Reference

```bash
make setup
make start
make start.{orange|pink}
make start.{st|sb}.{bq|pg}
make decrypt.{dev|staging|prod}
make encrypt.{dev|staging|prod}
make reset
make grpc.protoc
make grpc.protoc.bq
make deploy.staging.{main|versioned}
make deploy.prod.versioned
make tag-versioned
make ssl.{prod|staging|telegraf}
```

## Release Management

Use `scripts/bump-release` (Python 3, standard library only) to prepare a release:

```bash
# Preview, then update VERSION and helm/Chart.yaml appVersion.
# The chart version receives a patch bump by default.
scripts/bump-release 1.54.0 --dry-run
scripts/bump-release 1.54.0

# Override the chart version when a minor or major chart release is needed.
scripts/bump-release 1.54.0 --chart-version 0.7.0

# Release chart changes without changing the application version.
scripts/bump-release --chart-only
scripts/bump-release --chart-only --chart-version 0.7.0 --dry-run

# Validate the current versions without writing files (also run in Helm CI).
scripts/bump-release --check
```

The bump examples are alternatives, not a sequence. Versions must be stable
`X.Y.Z` with no leading zeros; prerelease and build suffixes are not supported.
Application and chart bumps must increase their respective versions. Existing
`VERSION` / `appVersion` drift or malformed version fields fail before any
writes; resolve the mismatch before bumping. The script works from any working
directory when invoked by its path and preserves chart comments and unrelated
fields. It does not modify `helm/values.yaml`: the default image tag already
uses `appVersion`. Review the diff and commit the changes through the usual PR
workflow; the script does not commit, tag, push, or publish a release.

Run the dependency-free CLI tests with:

```bash
python3 -B -m unittest discover -s scripts -p test_bump_release.py -v
```

The `master` branch reflects what is on production on <https://logflare.app>

The `staging` branch reflects what is on the staging environment.

Version bumping policy is as follows:

- **Patch**: For any changes that do not affect external API. UI changes,
  refactoring, docs, etc. Is the default.
- **Minor**: Non-breaking external API changes, changes in config, major changes
  to core features.
- **Major**: External API breaking changes, major code changes.

## Development Code Style

### Logging

Use the `:error_string` metadata key when logging, which is for additional
information that we want to log but don't necessarily want searchable or parsed
for schema updating.

For example, do `Logger.error("Some error", error_string: inspect(params))`

## Troubleshooting

### Many `mix test` tests failing

`mix test` only creates/migrates the test DB; it does not clear existing data.
If your `logflare_test` db was left in an intermediate state, it's possible that old state will cause tests to fail.
The fix is to reset the test database.

```bash
# reset
mise exec -- env MIX_ENV=test mix ecto.reset

# run tests again
mise exec -- mix test
```

## Deprecations

### users.bigquery_udfs_hash

The `bigquery_udfs_hash` column on the `users` table is deprecated. It is no
longer referenced in the `Logflare.User` schema and will be dropped from the `users`
table in a future release
