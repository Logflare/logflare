use Mix.Config

config :logflare, env: :staging

config :logflare, LogflareWeb.Endpoint,
  http: [
    port: 4_000,
    transport_options: [
      max_connections: 16_384,
      num_acceptors: 10,
      socket_opts: [{:raw, 1, 15, <<1::32-native>>}]
    ],
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
  url: [host: "logflarestaging.com", scheme: "https", port: 443],
  cache_static_manifest: "priv/static/cache_manifest.json",
  check_origin: [
    "https://logflarestaging.com",
    "//*.logflarestaging.com"
  ],
  server: true,
  code_reloader: false,
  version: Application.spec(:logflare, :vsn)

config :logger,
  level: :info,
  backends: [:console]

config :logger, :console, metadata: :all

config :phoenix, :serve_endpoints, true

config :logflare, Logflare.Repo,
  port: 5432,
  pool_size: 5,
  ssl: true,
  prepare: :unnamed,
  timeout: 30_000

config :logflare, Logflare.Google,
  dataset_id_append: "_staging",
  project_number: "395392434060",
  project_id: "logflare-staging",
  service_account: "logflare-staging@logflare-staging.iam.gserviceaccount.com",
  compute_engine_sa: "compute-engine-2022@logflare-staging.iam.gserviceaccount.com",
  api_sa: "395392434060@cloudservices.gserviceaccount.com",
  cloud_build_sa: "395392434060@cloudbuild.gserviceaccount.com"

config :logflare_telemetry,
  source_id: :"e5d18201-f0e0-459b-b6b3-2d3bc7d16fa4"

config :libcluster,
  debug: true,
  topologies: [
    gce: [
      strategy: Logflare.Cluster.Strategy.GoogleComputeEngine,
      config: [
        release_name: :logflare
      ]
    ]
  ]

config :logflare, Logflare.Cluster.Strategy.GoogleComputeEngine,
  regions: [{"us-central1", "logflare-cluster-group"}],
  zones: [
    {"us-central1-a", "instance-group-1"},
    {"us-central1-a", "instance-group"}
  ]

config :logflare, Logflare.Tracker, pool_size: 1

config :logflare, Logflare.Cluster.Utils, min_cluster_size: 1

import_config "telemetry.exs"

if File.exists?("config/staging.secret.exs") do
  import_config "staging.secret.exs"
end
