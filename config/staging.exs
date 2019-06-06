use Mix.Config

config :logflare, LogflareWeb.Endpoint,
  http: [port: 4000, transport_options: [max_connections: "infinity", num_acceptors: 10]],
  url: [host: "logflarestaging.com", scheme: "https"],
  cache_static_manifest: "priv/static/cache_manifest.json",
  server: true,
  code_reloader: false,
  version: Application.spec(:logflare, :vsn)

config :logger, :console, format: "[$level] $message\n"
config :logger, level: :error

config :phoenix, :serve_endpoints, true

config :logflare, Logflare.Repo,
  pool_size: 15,
  ssl: true,
  prepare: :unnamed,
  timeout: 30_000

config :logflare, Logflare.Google,
  dataset_id_append: "_staging",
  project_number: "395392434060",
  project_id: "logflare-staging",
  service_account: "logflare-staging@logflare-staging.iam.gserviceaccount.com"

config :logflare, env: :staging

import_config "staging.secret.exs"
