require Logger
import Config

config :logflare, env: :prod

config :logflare, LogflareWeb.Endpoint,
  http: [
    port: 4000,
    thousand_island_options: [
      num_acceptors: 1_000,
    ],
    # force high throguhput connections to re-connect to diff node
    http_1_options: [max_requests: 1_000_000],
    http_2_options: [max_requests: 1_000_000]
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
  sync_threshold: 10_000,
  discard_threshold: 10_000,
  compile_time_purge_matching: [
    [level_lower_than: :info]
  ]

config :logflare_logger_backend,
  flush_interval: 2_000,
  max_batch_size: 250
