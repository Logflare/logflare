use Mix.Config

config :logflare, LogflareWeb.Endpoint,
  http: [port: 4000, transport_options: [max_connections: "infinity", num_acceptors: 100]],
  url: [host: "logflare.app", scheme: "https"],
  cache_static_manifest: "priv/static/cache_manifest.json",
  server: true,
  code_reloader: false,
  version: Application.spec(:logflare, :vsn)

config :logger,
  level: :error,
  backends: [LogflareLogger.HttpBackend]

config :phoenix, :serve_endpoints, true

config :logflare, Logflare.Repo,
  pool_size: 15,
  ssl: true,
  prepare: :unnamed,
  timeout: 30_000,
  queue_target: 5000

config :logflare, Logflare.Google,
  dataset_id_append: "_prod",
  project_number: "1074203751359",
  project_id: "logflare-232118",
  service_account: "logflare@logflare-232118.iam.gserviceaccount.com"

config :logflare, env: :prod

import_config "prod.secret.exs"
