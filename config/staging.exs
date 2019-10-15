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
  service_account: "logflare-staging@logflare-staging.iam.gserviceaccount.com",
  compute_engine_sa: "395392434060-compute@developer.gserviceaccount.com",
  api_sa: "395392434060@cloudservices.gserviceaccount.com",
  cloud_build_sa: "395392434060@cloudbuild.gserviceaccount.com",
  gcp_cloud_build_sa: "service-395392434060@gcp-sa-cloudbuild.iam.gserviceaccount.com",
  compute_system_iam_sa: "service-395392434060@compute-system.iam.gserviceaccount.com",
  container_engine_robot_sa:
    "service-395392434060@container-engine-robot.iam.gserviceaccount.com",
  dataproc_sa: "service-395392434060@dataproc-accounts.iam.gserviceaccount.com",
  container_registry_sa: "service-395392434060@containerregistry.iam.gserviceaccount.com",
  redis_sa: "service-395392434060@cloud-redis.iam.gserviceaccount.com",
  serverless_robot_sa: "service-395392434060@serverless-robot-prod.iam.gserviceaccount.com",
  service_networking_sa: "service-395392434060@service-networking.iam.gserviceaccount.com",
  source_repo_sa: "service-395392434060@sourcerepo-service-accounts.iam.gserviceaccount.com"

config :logflare_logger_backend,
  api_key: "aaaaa",
  source_id: "bbbbbb",
  flush_interval: 1_000,
  max_batch_size: 50,
  url: "http://example.com"

config :logflare_telemetry,
  source_id: :"e5d18201-f0e0-459b-b6b3-2d3bc7d16fa4"

config :libcluster,
  debug: true,
  topologies: [
    k8s_chat: [
      strategy: Elixir.Cluster.Strategy.Kubernetes.DNS,
      config: [
        service: "logflare-staging-headless",
        application_name: "logflare",
        polling_interval: 1_000
      ]
    ]
  ]

# config :libcluster,
#   topologies: [
#     k8s_example: [
#       strategy: Elixir.Cluster.Strategy.Kubernetes,
#       config: [
#         mode: :ip,
#         kubernetes_node_basename: "logflare",
#         kubernetes_selector: "app=logflare-staging",
#         kubernetes_namespace: "default",
#         polling_interval: 1_000
#       ]
#     ]
#   ]

config :logflare, Logflare.Tracker, pool_size: 1

import_config "telemetry.exs"

if File.exists?("config/staging.secret.exs") do
  import_config "staging.secret.exs"
end
