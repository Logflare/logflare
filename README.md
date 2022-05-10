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

## Closed Source Usage

Logflare is using a SQL parser from sqlparser.com. To set this up on your dev machine:

- Copy parser from sqlparser.com into `sql/gsp`. When extracted it's located at `lib/gudusoft.gsqlparser-2.5.2.5.jar`
- Install Java with homebrew (MacOS) by running `brew install cask java`
- Run `mix sql`

## Dev Setup

1. Copy over secrets to two locations
   1. Dev secrets - `configs/dev.secret.exs`
   2. Google JWT key - `config/secrets/logflare-dev-238720-63d50e3c9cc8.json`
2. Start database `docker-compose up -d`
3. Run `mix setup` for deps, migrations, and seed data.
4. Restart your postgres server for replication settings to take effect `docker-compose restart`
5. Run `(cd assets; yarn)` from project root, to install js dependencies
6. Install `sqlparser` by following the steps in **Closed Source Usage** section.
7. Start server`mix start`
8. Sign in as a user
9. Create a source
10. Update `dev.secrets.exs`, search for the `:logflare_logger_backend` config and update the user api key and source id
11. Set user api key can be retrieved from dashboard or from database `users` table, source id is from the source page
12. In `iex` console, test that everything works:

```elixir
iex> LogflareLogger.info("testing.123")
```
