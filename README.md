# Logflare

## About

Stream logs to a central service and tail them in your browser. Logflare is different because you can **bring your own backend**. Simply provide your BigQuery credentials and we stream logs into your BigQuery table while automatically managing the schema.

Logflare is now a part of [Supabase](https://github.com/supabase/supabase).

Sign up at https://logflare.app.

![Logflare Example Gif](https://logflare.app/images/logflare-example.gif)

## Integrations

| Provider/Runtime | Link                                                                                                                                                                                           |
| ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Cloudflare       | <a href="https://www.cloudflare.com/apps/logflare/install"><img src="https://install.cloudflareapps.com/install-button.png" alt="Install Logflare with Cloudflare" border="0" width="110"></a> |
| Vercel           | [Logflare Vercel integration](https://vercel.com/integrations/logflare)                                                                                                                        |
| Fly              | [Logflare/fly-log-shipper](https://github.com/Logflare/fly-log-shipper)                                                                                                                        |
| Github Action    | [Logflare/action](https://github.com/Logflare/action)                                                                                                                                          |
| Javascript       | [Logflare/pino-transport](https://github.com/Logflare/pino-logflare)                                                                                                                           |
| Javascript       | [Logflare/winston-logflare](https://github.com/Logflare/winston-logflare)                                                                                                                      |
| Elixir           | [Logflare/logflare_logger_backend](https://github.com/Logflare/logflare_logger_backend)                                                                                                        |
| Elixir           | [Logflare/logflare_agent](https://github.com/Logflare/logflare_agent)                                                                                                                          |
| Erlang           | [Logflare/logflare_erl](https://github.com/Logflare/logflare_erl)                                                                                                                              |

## Learn more

- [Official website](https://logflare.app)
- [Guides](https://logflare.app/guides) and [documentation](https://docs.logflare.app)
- <support@logflare.app> or <support@supabase.com>

## Developer

### Env Setup

1. Install dependencies with `asdf` using `asdf install`
1. Copy over secrets to two locations
   1. Dev secrets - `.dev.env`
   2. Google JWT key - `.gcloud.json`
1. Start database and stripe mock `docker compose up -d db stripe-mock`
1. Run `mix setup` for deps, migrations, and seed data.
1. Run `(cd assets; npm i)` from project root, to install js dependencies
1. Install `sqlparser` by following the steps in **Closed Source Usage** section.
1. Start server`mix setup`
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
