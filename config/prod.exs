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

config :logger,
  compile_time_purge_matching: [
    [level_lower_than: :info]
  ]

config :logflare_logger_backend,
  flush_interval: 2_000,
  max_batch_size: 250

config :logflare, :spool,
  mode: :disable,
  bucket: "logflare-spool",
  partitions: 1,
  flush_delay_ms: 100,
  wal_max_rotation_interval_ms: 1_000,
  compress: true,
  queue_name: "logflare-spool"
