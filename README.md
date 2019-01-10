# Logflare

## About

Stream logs to a central service and tail them in your browser.

Sign up at https://loglare.app.

## For Cloudflare

You can setup a worker in your own account.

If you sign up, when you create a new source you can copy the Cloudflare worker code from that page in the app and deploy a worker in your own account. You can copy and paste this without any modification as your API key and source key are customized for you there.

We currently don't have an app on Cloudflare. Working on one though, see the cloudflare-app repo.

## For log files

See the logflare-agent repo to send over logs from files. It's an elixir app that watches a file (soon multiple files) and sends over any new lines it finds via an API call.

## Setup

To start your Phoenix server:

  * Install dependencies with `mix deps.get`
  * Create and migrate your database with `mix ecto.create && mix ecto.migrate`
  * Install Node.js dependencies with `cd assets && npm install`
  * Start Phoenix endpoint with `mix phx.server`

Now you can visit [`localhost:4000`](http://localhost:4000) from your browser.

Ready to run in production? Please [check our deployment guides](http://www.phoenixframework.org/docs/deployment).

## Learn more

  * Official website: http://www.phoenixframework.org/
  * Guides: http://phoenixframework.org/docs/overview
  * Docs: https://hexdocs.pm/phoenix
  * Mailing list: http://groups.google.com/group/phoenix-talk
  * Source: https://github.com/phoenixframework/phoenix
