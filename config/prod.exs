require Logger
use Mix.Config

config :logflare, env: :prod

config :logflare, LogflareWeb.Endpoint,
  http: [port: 4000, transport_options: [max_connections: 16384, num_acceptors: 100]],
  url: [host: "logflare.app", scheme: "https"],
  cache_static_manifest: "priv/static/cache_manifest.json",
  server: true,
  code_reloader: false,
  version: Application.spec(:logflare, :vsn)

config :logger,
  level: :info,
  backends: [LogflareLogger.HttpBackend]

config :phoenix, :serve_endpoints, true

config :logflare, Logflare.Repo,
  pool_size: 90,
  ssl: true,
  prepare: :unnamed,
  timeout: 30_000,
  queue_target: 5_000

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

config :libcluster,
  topologies: [
    # dev: [
    #   strategy: Cluster.Strategy.Epmd,
    #   config: [
    #     hosts: [:"pink@Chases-MBP-2017", :"orange@Chases-MBP-2017", :"red@Chases-MBP-2017"]
    #   ]
    # ]
    gossip_example: [
      strategy: Elixir.Cluster.Strategy.Gossip,
      config: [
        port: 45892,
        if_addr: "0.0.0.0",
        multicast_addr: "230.1.1.251",
        multicast_ttl: 1,
        secret: "somepassword"
      ]
    ]
  ]

config :logflare, Logflare.Tracker, pool_size: 50

import_config "telemetry.exs"

if File.exists?("config/prod.secret.exs") do
  Logger.info("prod.secret.exs found, importing..")
  import_config("prod.secret.exs")
else
  Logger.warn("prod.secret.exs doesn't exist")
end
