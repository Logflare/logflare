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

## Release Management

Logflare's `VERSION` file is bumped on each release.

The `master` branch reflects what is on production on https://logflare.app

The `staging` branch reflects what is on the staging environment.

Version bumping policy is as follows:

- **Patch**: For any changes that do not affect external api. ui changes, refactoring, docs, etc. Is the default.
- **Minor**: Non-breaking external api changes, changes in config, major changes to core features.
- **Major**: External api breaking changes, major code changes.

## Development Code Style

### Logging

Use the `:error_string` metadata key when logging, which is for additional information that we want to log but don't necessarily want searchable or parsed for schema updating.

For example, do `Logger.error("Some error", error_string: inspect(params) )`

## Deployment

### GRPC Server

To deploy the GRPC server in a new environment you need the following steps for your `<env>`:

- **Cloudflare:** Enable GRPC in the `Network` tab
- **Cloudflare:** Generate a CA Certificate to be used on your Origin server:
  - SSL/TLS -> Origin Server -> Create Certificate -> Create
  - Create .`<env>`.cacert.pem with the content from the certificate field
  - Create .`<env>`.cacert.key with the content from the key field
- **Local:** Generate a self signed certificate for the origin server:
  - `openssl req -newkey rsa:2048 -nodes -days 365000 -keyout .<env>.cert.key -out .<env>.req.pem` and set the email to your support email
  - `openssl x509 -req -days 12783 -set_serial 1 -in .<env>.req.pem -out .<env>.cert.pem -CA .<env>.cacert.pem -CAkey .<env>.cacert.key`
  - **Be extremely careful with the generated files**
  - Store this files to be pushed into the server
- **Google Cloud:** On your `Instance Template`, allow for HTTPS traffic in the Firewall configuration
- **Google Cloud:** On your `Instance Group`, add a new port onto your `Port Mapping` configuration to be `50051` (do check you change all `Instance Groups`)
- **Google Cloud:** Create your Load Balancer
  - Select `HTTP(S) load balancing`
  - Select `From Internet to my VMs or serverless services` and `Global HTTP(S) load balancer`
  - Frontend Configuration
    - Protocol - `HTTPS (includes HTTP/2)`
    - IP Address - Create a new IP Address
    - Set certificate
  - Backend Configuration
    - Create `Backend Service` for each `Instance Group` you want to support
    - Select the target `Instance Group` and select the GRPC Port set earlier in the popup
    - Disable Cloud CDN
    - Enable Logging
    - Set Health check
- **Cloudflare:** Set a new DNS route with a sub domain pointing to the generated IP of your GRPC LB
