import Config

config :logflare, env: :dev, supabase_mode: true

config :logflare, LogflareWeb.Endpoint,
  server: true,
  debug_errors: true,
  code_reloader: false,
  watchers: [
    npm: [
      "run",
      "watch",
      cd: Path.expand("../assets", __DIR__),
      env: [{"NODE_ENV", "development"}]
    ]
  ],
  live_reload: [
    patterns: [
      ~r{priv/static/.*(js|css|png|jpeg|jpg|gif|svg)$},
      ~r{priv/gettext/.*(po)$},
      ~r{lib/logflare_web/views/.*(ex)$},
      ~r{lib/logflare_web/templates/.*(eex)$},
      ~r{lib/logflare_web/live/.*(ex)$}
    ]
  ]

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
  database: "logflare_dev"
