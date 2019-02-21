# Logflare

## About

Stream logs to a central service and tail them in your browser.

![Logflare Example Gif](https://logflare.app/images/logflare-example.gif)

Sign up at https://logflare.app.

## For Cloudflare

Install the Cloudflare App.

<a href="https://www.cloudflare.com/apps/logflare/install?source=button">
  <img
    src="https://install.cloudflareapps.com/install-button.png"
    alt="Install Logflare with Cloudflare"
    border="0"
    width="150">
</a>

## For log files

See the logflare-agent repo to send over logs from files. It's an elixir app that watches or multiple files and sends over any new lines it finds via an API call.

## Learn more

  * Official website: https://logflare.app
  * Cloudflare app: https://www.cloudflare.com/apps/logflare/install
  * Support: https://twitter.com/chasers

## Self hosted

Make sure Erlang, Elixir and Postgres are installed. Clone the repo then start your Phoenix server:

  * Install dependencies with `mix deps.get`
  * Create and migrate your database with `mix ecto.create && mix ecto.migrate`
  * Install Node.js dependencies with `cd assets && npm install`
  * Start Phoenix endpoint with `mix phx.server`

Now you can visit [`localhost:4000`](http://localhost:4000) from your browser.
