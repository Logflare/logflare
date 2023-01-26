# Logflare

## About

Stream logs to a central service and tail them in your browser. Logflare is different because you can **bring your own backend**. Simply provide your BigQuery credentials and we stream logs into your BigQuery table while automatically managing the schema.

Sign up at https://logflare.app.

![Logflare Example Gif](https://logflare.app/images/logflare-example.gif)

## For Cloudflare

Automatically log structured request/response data in a few clicks with the Cloudflare app.

<a href="https://www.cloudflare.com/apps/logflare/install?source=button">
  <img
    src="https://install.cloudflareapps.com/install-button.png"
    alt="Install Logflare with Cloudflare"
    border="0"
    width="150">
</a>

## For Vercel

Setup the [Logflare Vercel integration](https://vercel.com/integrations/logflare) and we'll automatically structure your Vercel logs.

## For Javascript

Use [our Pino transport](https://github.com/Logflare/pino-logflare) to log structured data and exceptions straight from your Javascript project.

## For Elixir

Use [our Logger backend](https://github.com/Logflare/logflare_logger_backend) to send your Elixir exceptions and structured logs to Logflare.

## Learn more

- Official website: https://logflare.app
- All our guides: https://logflare.app/guides
- Support: https://twitter.com/logflare_logs or support@logflare.app

## Source available

We are leaving this repo public as an example of a larger Elixir project. We hope to have an open source edition of Logflare at some point in the future.

## Developer

### Env Setup

1. Install dependencies with `asdf` using `asdf install`
1. Copy over secrets to two locations
   1. Dev secrets - `.dev.env` (core team: run `mix decrypt.dev` to setup local environment variables.)
   2. Google JWT key - `.gcloud.json`
1. Start database `docker-compose up -d db`
1. Run `mix setup` for deps, migrations, and seed data.
1. Run `(cd assets; yarn)` from project root, to install js dependencies
1. Start server`mix start`
1. Sign in as a user
1. Create a source
1. Update `.dev.env`, search for the `LOGFLARE_LOGGER_BACKEND_API_KEY` and `LOGFLARE_LOGGER_BACKEND_SOURCE_ID` and update them accordingly
1. Set user api key can be retrieved from dashboard or from database `users` table, source id is from the source page
1. In `iex` console, test that everything works:

```elixir
iex> LogflareLogger.info("testing log message")
```

### Using Docker

1. Build images with `docker compose build`
2. Run with `docker compose up -d`

### Logging

Use the `:error_string` metadata key when logging, which is for additional information that we want to log but don't necessarily want searchable or parsed for schema updating.

For example, do `Logger.error("Some error", error_string: inspect(params) )`
