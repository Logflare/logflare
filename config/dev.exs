use Mix.Config

config :logflare, env: :dev

config :logflare, LogflareWeb.Endpoint,
  http: [
    port: System.get_env("PORT") || 4000,
    transport_options: [max_connections: 16384, num_acceptors: 10]
  ],
  url: [host: "dev.chasegranberry.net", scheme: "http"],
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

config :logflare, LogflareWeb.Endpoint,
  live_reload: [
    patterns: [
      ~r{priv/static/.*(js|css|png|jpeg|jpg|gif|svg)$},
      ~r{priv/gettext/.*(po)$},
      ~r{lib/logflare_web/views/.*(ex)$},
      ~r{lib/logflare_web/templates/.*(eex)$},
      ~r{lib/logflare_web/live/.*(ex)$}
    ]
  ]

config :logger, :console, format: "[$level] $message\n"

# config :logger,
#   level: :info,
#   backends: [LogflareLogger.HttpBackend]

config :logger,
  level: :debug

config :phoenix, :stacktrace_depth, 20

config :logflare, Logflare.Repo,
  username: "chasegranberry",
  password: "",
  database: "logtail_dev",
  hostname: "localhost",
  pool_size: 10,
  prepare: :unnamed,
  log: false

config :logflare, Logflare.Google,
  dataset_id_append: "_dev",
  project_number: "1023172132421",
  project_id: "logflare-dev-238720",
  service_account: "logflare-dev@logflare-dev-238720.iam.gserviceaccount.com"


config :libcluster,
  debug: true,
  topologies: [
    dev: [
      strategy: Cluster.Strategy.Epmd,
      config: [hosts: [:"a@127.0.0.1", :"b@127.0.0.1"]],
    ]
  ]


import_config "dev.secret.exs"
