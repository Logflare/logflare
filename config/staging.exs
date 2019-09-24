use Mix.Config

config :logflare, env: :staging

config :logflare, LogflareWeb.Endpoint,
  http: [port: 4_000, transport_options: [max_connections: 16_384, num_acceptors: 10]],
  url: [host: "logflarestaging.com", scheme: "https"],
  cache_static_manifest: "priv/static/cache_manifest.json",
  check_origin: false,
  server: true,
  code_reloader: false,
  version: Application.spec(:logflare, :vsn)

config :logger, :console, format: "[$level] $message\n"

config :logger,
  level: :info

config :phoenix, :serve_endpoints, true

config :logflare, Logflare.Repo,
  pool_size: 5,
  ssl: true,
  prepare: :unnamed,
  timeout: 30_000

config :logflare, Logflare.Google,
  dataset_id_append: "_staging",
  project_number: "395392434060",
  project_id: "logflare-staging",
  service_account: "logflare-staging@logflare-staging.iam.gserviceaccount.com"

config :logflare_logger_backend,
  api_key: "aaaaa",
  source_id: "bbbbbb",
  flush_interval: 1_000,
  max_batch_size: 50,
  url: "http://example.com"

config :logflare_telemetry,
  source_id: :"00a9997c-3e77-4220-bdb7-5f63be2247f5"

config :libcluster,
  debug: true,
  topologies: [
    k8s_chat: [
      strategy: Elixir.Cluster.Strategy.Kubernetes.DNS,
        config: [
          service: "logflare-staging-headless",
          application_name: "logflare"
        ]
      ]
    ]

if File.exists?("config/staging.secret.exs") do
  import_config "staging.secret.exs"
end
