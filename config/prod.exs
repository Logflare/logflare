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
  url: [host: "logflare.app", scheme: "https", port: 443],
  cache_static_manifest: "priv/static/cache_manifest.json",
  server: true,
  code_reloader: false,
  check_origin: [
    "https://logflare.app",
    "//*.logflare.app"
  ],
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
  pool_size: 90,
  ssl: true,
  prepare: :unnamed,
  timeout: 30_000,
  queue_target: 5_000,
  database: "logflare",
  hostname: "10.11.145.35",
  port: 5432

config :logflare, Logflare.Google,
  # gcloud services enable cloudbuild.googleapis.com container.googleapis.com dataproc.googleapis.com redis.googleapis.com cloudfunctions.googleapis.com run.googleapis.com servicenetworking.googleapis.com sourcerepo.googleapis.com
  dataset_id_append: "_prod",
  project_number: "1074203751359",
  project_id: "logflare-232118",
  service_account: "logflare@logflare-232118.iam.gserviceaccount.com",
  compute_engine_sa: "compute-engine-2022@logflare-232118.iam.gserviceaccount.com",
  api_sa: "1074203751359@cloudservices.gserviceaccount.com",
  cloud_build_sa: "1074203751359@cloudbuild.gserviceaccount.com"

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
  ],
  url: "https://api.logflarestaging.com"

config :logflare_logger_backend,
  source_id: "4593c8b8-be2c-4bc6-a3e7-2bf090dd501f",
  flush_interval: 2_000,
  max_batch_size: 250,
  url: "https://api.logflarestaging.com"

config :libcluster,
  topologies: [
    gce: [
      strategy: Logflare.Cluster.Strategy.GoogleComputeEngine,
      config: [
        release_name: :logflare
      ]
    ]
  ]

config :logflare, Logflare.Cluster.Strategy.GoogleComputeEngine,
  regions: [
    # {"us-central1", "logflare-prod-cluster-group"}
  ],
  zones: [
    {"us-central1-a", "logflare-prod-us-central1-a"},
    {"us-central1-a", "logflare-prod-us-central1-a-preempt"},
    {"us-central1-b", "logflare-prod-us-central1-b"},
    {"us-central1-b", "logflare-prod-us-central1-b-preempt"},
    {"us-central1-c", "logflare-prod-us-central1-c"},
    {"us-central1-c", "logflare-prod-us-central1-c-preempt"},
    {"us-central1-f", "logflare-prod-us-central1-f"},
    {"us-central1-f", "logflare-prod-us-central1-f-preempt"},
    {"europe-west3-a", "logflare-prod-eu-west3-a"},
    {"europe-west3-b", "logflare-prod-eu-west3-b"},
    {"europe-west3-c", "logflare-prod-eu-west3-c"}
  ]

config :logflare, Logflare.Tracker, pool_size: 5

config :logflare, LogflareWeb.Auth.VercelAuth,
  vercel_app_host: "https://vercel.logflare.app",
  client_id: "4aaf19555a5113ca0ecbcb93b7368daf700d5e6df0cbd8a1772ab442417486b0"

config :logflare, Logflare.Vercel.Client,
  client_id: "oac_yEwf1AmqJMbRs2rkmnePdNK3",
  redirect_uri: "https://logflare.app/install/vercel-v2",
  install_vercel_uri: "https://vercel.com/integrations/logflare/new"

config :erlexec, root: true, user: "root"

config :logflare, Logflare.Cluster.Utils, min_cluster_size: 2

import_config "telemetry.exs"

if File.exists?("config/prod.secret.exs") do
  Logger.info("prod.secret.exs found, importing..")
  import_config("prod.secret.exs")
else
  Logger.warn("prod.secret.exs doesn't exist")
end
