require Logger
use Mix.Config

config :logflare, env: :prod

config :logflare, LogflareWeb.Endpoint,
  http: [
    port: 4000,
    transport_options: [
      max_connections: 64_000,
      num_acceptors: 100,
      # for so_reuseport
      socket_opts: [{:raw, 1, 15, <<1::32-native>>}]
    ],
    # https://blog.percy.io/tuning-nginx-behind-google-cloud-platform-http-s-load-balancer-305982ddb340
    # https://github.com/ninenines/cowboy/issues/1286#issuecomment-699643478
    protocol_options: [
      # https://ninenines.eu/docs/en/cowboy/2.8/manual/cowboy_http/
      request_timeout: 30_000,
      # https://cloud.google.com/load-balancing/docs/https/#timeouts_and_retries
      # must be greater than 600s
      idle_timeout: 650_000,
      max_keepalive: :infinity
    ],
    compress: true
  ],
  cache_static_manifest: "priv/static/cache_manifest.json",
  server: true,
  code_reloader: false,
  version: Application.spec(:logflare, :vsn)

config :logger,
  level: :info,
  sync_threshold: 10_001,
  discard_threshold: 10_000,
  backends: [LogflareLogger.HttpBackend],
  compile_time_purge_matching: [
    [level_lower_than: :info]
  ]

config :phoenix, :serve_endpoints, true

config :logflare, Logflare.Repo,
  prepare: :unnamed,
  timeout: 30_000,
  queue_target: 5_000,
  port: 5432

config :logflare_agent,
  sources: [
    %{
      path: "/home/logflare/app_release/logflare/var/log/erlang.log.1",
      source: "4ec9216e-a8e9-46eb-92cb-1576092c9e4b"
    },
    %{
      path: "/home/logflare/app_release/logflare/var/log/erlang.log.2",
      source: "4ec9216e-a8e9-46eb-92cb-1576092c9e4b"
    },
    %{
      path: "/home/logflare/app_release/logflare/var/log/erlang.log.3",
      source: "4ec9216e-a8e9-46eb-92cb-1576092c9e4b"
    },
    %{
      path: "/home/logflare/app_release/logflare/var/log/erlang.log.4",
      source: "4ec9216e-a8e9-46eb-92cb-1576092c9e4b"
    },
    %{
      path: "/home/logflare/app_release/logflare/var/log/erlang.log.5",
      source: "4ec9216e-a8e9-46eb-92cb-1576092c9e4b"
    }
  ]

config :logflare_logger_backend,
  flush_interval: 2_000,
  max_batch_size: 250

config :libcluster,
  topologies: [
    gce: [
      strategy: Logflare.Cluster.Strategy.GoogleComputeEngine,
      config: [
        release_name: :logflare
      ]
    ]
  ]

config :logflare, Logflare.Tracker, pool_size: 5

import_config "telemetry.exs"

if File.exists?("config/prod.secret.exs") do
  Logger.info("prod.secret.exs found, importing..")
  import_config("prod.secret.exs")
else
  Logger.warn("prod.secret.exs doesn't exist")
end
