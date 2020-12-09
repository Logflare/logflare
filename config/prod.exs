require Logger
use Mix.Config

config :logflare, env: :prod

config :logflare, LogflareWeb.Endpoint,
  http: [
    port: 4000,
    transport_options: [max_connections: 16_384, num_acceptors: 100],
    protocol_options: [
      max_keepalive: 1_000_000,
      idle_timeout: 60_000,
      inactivity_timeout: 620_000
    ]
  ],
  url: [host: "logflare.app", scheme: "https", port: 443],
  cache_static_manifest: "priv/static/cache_manifest.json",
  server: true,
  code_reloader: false,
  version: Application.spec(:logflare, :vsn)

config :logger,
  level: :info,
  discard_threshold: 10_000,
  backends: [LogflareLogger.HttpBackend],
  compile_time_purge_matching: [
    [level_lower_than: :info]
  ]

config :phoenix, :serve_endpoints, true

config :logflare, Logflare.Repo,
  pool_size: 90,
  prepare: :unnamed,
  timeout: 30_000,
  queue_target: 5_000,
  database: "logflare",
  hostname: "10.11.144.4"

config :logflare, Logflare.Google,
  # gcloud services enable cloudbuild.googleapis.com container.googleapis.com dataproc.googleapis.com redis.googleapis.com cloudfunctions.googleapis.com run.googleapis.com servicenetworking.googleapis.com sourcerepo.googleapis.com
  dataset_id_append: "_prod",
  project_number: "1074203751359",
  project_id: "logflare-232118",
  service_account: "logflare@logflare-232118.iam.gserviceaccount.com",
  compute_engine_sa: "1074203751359-compute@developer.gserviceaccount.com",
  api_sa: "1074203751359@cloudservices.gserviceaccount.com",
  cloud_build_sa: "1074203751359@cloudbuild.gserviceaccount.com",
  gcp_cloud_build_sa: "service-1074203751359@gcp-sa-cloudbuild.iam.gserviceaccount.com",
  compute_system_iam_sa: "service-1074203751359@compute-system.iam.gserviceaccount.com",
  container_engine_robot_sa:
    "service-1074203751359@container-engine-robot.iam.gserviceaccount.com",
  dataproc_sa: "service-1074203751359@dataproc-accounts.iam.gserviceaccount.com",
  container_registry_sa: "service-1074203751359@containerregistry.iam.gserviceaccount.com",
  redis_sa: "service-1074203751359@cloud-redis.iam.gserviceaccount.com",
  serverless_robot_sa: "service-1074203751359@serverless-robot-prod.iam.gserviceaccount.com",
  service_networking_sa: "service-1074203751359@service-networking.iam.gserviceaccount.com",
  source_repo_sa: "service-1074203751359@sourcerepo-service-accounts.iam.gserviceaccount.com"

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
  flush_interval: 1_000,
  max_batch_size: 50,
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
    {"us-central1-c", "logflare-prod-us-central1-c"},
    {"us-central1-f", "logflare-prod-us-central1-f"},
    {"europe-west3-a", "logflare-prod-eu-west3-a"},
    {"europe-west3-b", "logflare-prod-eu-west3-b"},
    {"europe-west3-c", "logflare-prod-eu-west3-c"}
  ]

config :logflare, Logflare.Tracker, pool_size: 5

config :logflare, LogflareWeb.Auth.VercelAuth,
  vercel_app_host: "https://vercel.logflare.app",
  client_id: "4aaf19555a5113ca0ecbcb93b7368daf700d5e6df0cbd8a1772ab442417486b0"

config :logflare,
  recaptcha_site_key: "6LffD8sZAAAAAFPKl-dzyTTpPtunFXfL6Wm8zraT"

import_config "telemetry.exs"

if File.exists?("config/prod.secret.exs") do
  Logger.info("prod.secret.exs found, importing..")
  import_config("prod.secret.exs")
else
  Logger.warn("prod.secret.exs doesn't exist")
end
