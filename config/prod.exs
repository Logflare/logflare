require Logger
import Config

config :logflare, env: :prod

config :logflare, LogflareWeb.Endpoint,
  http: [
    port: 4000
  ],
  cache_static_manifest: "priv/static/cache_manifest.json",
  server: true,
  code_reloader: false,
  version: Application.spec(:logflare, :vsn)

config :phoenix, :serve_endpoints, true

config :logflare, Logflare.Repo,
  prepare: :unnamed,
  timeout: 30_000,
  queue_target: 5_000,
  port: 5432

config :logger, :default_handler,
  config: %{
    sync_mode_qlen: 10_000,
    drop_mode_qlen: 10_000
  }

config :logger,
  compile_time_purge_matching: [
    [level_lower_than: :info]
  ]

config :logflare_logger_backend,
  flush_interval: 2_000,
  max_batch_size: 250
