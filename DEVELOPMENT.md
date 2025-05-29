# Development

## Dev Env Setup

1. Run `make setup`, which will:
   1. Install dependencies with `asdf` using `asdf install`
2. Decrypt secrets with `make decrypt.dev`. It will decrypt two files:
   1. Dev secrets - `.dev.env`
   2. Google JWT key - `.gcloud.json`
3. Start dev dependencies: `docker compose up -d db stripe-mock`
4. Run `make setup` for dependencies, migrations, and seed data.
5. Start server with `make start`
6. Sign in as a user
7. Create a source
8. Update `.dev.env`, search for the `LOGFLARE_LOGGER_BACKEND_API_KEY` and
   `LOGFLARE_LOGGER_BACKEND_SOURCE_ID` and update them accordingly
9. Set user API key can be retrieved from dashboard or from database `users`
   table, source id is from the source page
10. In `iex` console, test that everything works:

```elixir
iex> LogflareLogger.info("testing log message")
```

## Docker Services

Run the local database with `docker-compose up -d db`.

To run the full docker setup, run `docker-compose up -d`. This will load the GCP
JWT and the `.docker.env`. Ensure that both files exist.

To build images only, use `docker-compose build`

## Supabase Development

### Developing in Supabase Mode

To run the dev env in Supabase mode use the following command:

```
docker compose -f docker-compose.yml -f docker-compose.supabase.yml up
```

This will tell Logflare to perform data seeding and disable UI auth.

This is useful for testing supabase mode and single tenant mode.

### Developing Logflare alongside Supabase CLI

In order to test all changes locally, perform the following steps:

1. Build logflare docker image locally: `docker-compose build`
   - the compose file tags the image locally.
2. CLI repo: run the CLI locally `go run . start`
   - prefix all CLI commands with `go run .`
   - run `go run . init` to create a local Supabase project
3. Update the test Supabase project's config under `supabase/config.toml`
   - Logflare uses the `analytics` namespace.

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
