# Development

## Dev Env Setup

1. Install dependencies with `asdf` using `asdf install`
2. Decrypt secrets with `mix decrypt.dev`. It will decrypt two files:
   1. Dev secrets - `.dev.env`
   2. Google JWT key - `.gcloud.json`
3. Start dev dependencies: `docker compose up -d db stripe-mock`
4. Run `mix setup` for deps, migrations, and seed data.
5. Run `(cd assets; npm i)` from project root, to install js dependencies
6. Start server with `mix start`
7. Sign in as a user
8. Create a source
9. Update `.dev.env`, search for the `LOGFLARE_LOGGER_BACKEND_API_KEY` and `LOGFLARE_LOGGER_BACKEND_SOURCE_ID` and update them accordingly
10. Set user api key can be retrieved from dashboard or from database `users` table, source id is from the source page
11. In `iex` console, test that everything works:

```elixir
iex> LogflareLogger.info("testing log message")
```

## Docker Services

Run the local database with `docker-compose up -d db`.

To run the full docker setup, run `docker-compose up -d`. This will load the GCP jwt and the `.docker.env`. Ensure that both files exist.

To build images only, use `docker-compose build`

## Supabase Development

### Developing in Supabase Mode

To run the dev env in Supabase mode, adjust the `.docker.env` config:

```
LOGFLARE_SINGLE_TENANT=true
LOGFLARE_SUPABASE_MODE=true
```

This will tell Logflare to perform data seeding and disable ui auth.

Thereafter, run `mix start.docker` to run the dev server with docker config. This is useful for testing supabase mode and single tenant mode.

### Developing Logflare alongside Supabase CLI

In order to test all changes locally, perform the following steps:

1. Build logflare docker image locally: `docker-compose build`
   - the compose file tags the image locally.
2. CLI repo: run the cli locally `go run . start`
   - prefix all cli commands with `go run .`
   - run `go run . init` to create a local supabase project
3. Update the test supabase project's config under `supabase/config.toml`
   - logflare uses the `analytics` namespace.

## Development Code Style

### Logging

Use the `:error_string` metadata key when logging, which is for additional information that we want to log but don't necessarily want searchable or parsed for schema updating.

For example, do `Logger.error("Some error", error_string: inspect(params) )`
