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
# install dependencies
make setup

# start local database
docker-compose up -d db clickhouse

# start in single tenant postgres backend
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
docker compose up db logflare

# to build locally with pg backend
docker compose -f docker-compose.yml -f docker-compose.pg.yml up db logflare

# to run latest image locally with bq backend
docker compose -f docker-compose.yml -f docker-compose.latest.yml up db logflare

# to run latest image locally with pg backend
docker compose -f docker-compose.yml -f docker-compose.latest.yml -f docker-compose.pg.yml up db logflare
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
make ssl.{prod|staging}
```

## Release Management

Logflare's `VERSION` file is bumped on each release.

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


## Deprecations

### users.bigquery_udfs_hash

The `bigquery_udfs_hash` column on the `users` table is deprecated. It is no
longer referenced in the `Logflare.User` schema and will be dropped from the `users`
table in a future release
