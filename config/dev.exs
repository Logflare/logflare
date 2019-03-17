use Mix.Config

# For development, we disable any cache and enable
# debugging and code reloading.
#
# The watchers configuration can be used to run external
# watchers to your application. For example, we use it
# with brunch.io to recompile .js and .css sources.
config :logflare, LogflareWeb.Endpoint,
  http: [port: 4000],
  url: [host: "localhost", port: 4000],
  debug_errors: true,
  code_reloader: true,
  check_origin: false,
  watchers: [
    node: [
      "node_modules/webpack/bin/webpack.js",
      "--mode",
      "development",
      "--watch-stdin",
      cd: Path.expand("../assets", __DIR__)
    ]
  ]

# SSL Support
#  config :logflare, LogflareWeb.Endpoint,
#  https: [:inet6,
#          port: 4443,
#          keyfile: "priv/keys/localhost.key",
#          certfile: "priv/keys/localhost.cert"],
#  force_ssl: [hsts: true]

# Watch static and templates for browser reloading.
config :logflare, LogflareWeb.Endpoint,
  live_reload: [
    patterns: [
      ~r{priv/static/.*(js|css|png|jpeg|jpg|gif|svg)$},
      ~r{priv/gettext/.*(po)$},
      ~r{lib/logflare_web/views/.*(ex)$},
      ~r{lib/logflare_web/templates/.*(eex)$}
    ]
  ]

# Do not include metadata nor timestamps in development logs
config :logger, :console, format: "[$level] $message\n"

# Set a higher stacktrace during development. Avoid configuring such
# in production as building large stacktraces may be expensive.
config :phoenix, :stacktrace_depth, 20

# Configure your database
config :logflare, Logflare.Repo,
  #  adapter: Ecto.Adapters.Postgres,
  username: "chasegranberry",
  password: "",
  database: "logtail_dev",
  hostname: "localhost",
  pool_size: 10,
  prepare: :unnamed

import_config "dev.secret.exs"
