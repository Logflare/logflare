use Mix.Config

config :logflare, env: :dev

config :logflare, LogflareWeb.Endpoint,
  server: true,
  http: [
    port: System.get_env("PORT") || 4000,
    transport_options: [
      max_connections: 16_384,
      num_acceptors: 100,
      socket_opts: [{:raw, 0xFFFF, 0x0200, <<1::32-native>>}]
    ],
    protocol_options: [max_keepalive: 1_000],
    compress: true
  ],
  live_view: [
    signing_salt: "eVpFFmpN+OHPrilThexLilWnF+a8zBLbCtdH/OzAayShcm1B3OHOyGiadM6qOezp"
  ],
  debug_errors: true,
  code_reloader: false,
  check_origin: false


config :logger,
  level: :debug


config :logger, :console,
  format: "\n[$level] [$metadata] $message\n",
  metadata: [:request_id],
  level: :debug

config :phoenix, :stacktrace_depth, 20

config :logflare, Logflare.Repo,
  username: "postgres",
  password: "postgres",
  database: "logflare_docker",
  hostname: "localhost",
  port: 5432,
  pool_size: 10,
  prepare: :unnamed,
  log: false
