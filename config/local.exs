import Config

config :erlexec, root: true, user: "root"

config :logflare, env: :local

config :logflare, Logflare.Repo,
  username: "postgres",
  password: "postgres",
  database: "logflare",
  hostname: "db",
  port: 5432,
  pool_size: 10,
  prepare: :unnamed,
  log: false

config :logflare, Logflare.Tracker, pool_size: 1

config :logflare, Logflare.Cluster.Utils, min_cluster_size: 1

config :logflare, LogflareWeb.Endpoint,
  http: [
    ip: {0, 0, 0, 0},
    port: 4000,
    transport_options: [
      max_connections: 16_384,
      num_acceptors: 100,
      socket_opts: [{:raw, 0xFFFF, 0x0200, <<1::32-native>>}]
    ],
    protocol_options: [max_keepalive: 1_000],
    compress: true
  ],
  debug_errors: false,
  code_reloader: false,
  check_origin: false,
  watchers: []

config :goth, json: File.read!(".google.secret.json")

config :logflare_logger_backend,
  flush_interval: 1_000,
  max_batch_size: 50,
  url: "http://localhost:4000"

config :libcluster, topologies: []
