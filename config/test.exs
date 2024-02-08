import Config

# We don't run a server during test. If one is required,
# you can enable the server option below.
config :logflare, LogflareWeb.Endpoint,
  http: [port: 4001],
  server: false

config :logflare,
  env: :test

config :logflare, Logflare.Cluster.Utils, min_cluster_size: 1

config :logger, :console, metadata: :all, level: :none

config :logflare, Logflare.Google,
  dataset_id_append: "_test",
  project_number: "1023172132421",
  project_id: "logflare-dev-238720",
  service_account: "logflare-dev@logflare-dev-238720.iam.gserviceaccount.com",
  compute_engine_sa: "GOOGLE_COMPUTE_ENGINE_SA",
  api_sa: "GOOGLE_API_SA",
  cloud_build_sa: "GOOGLE_CLOUD_BUILD_SA",
  cloud_build_trigger_sa: "GOOGLE_CLOUD_BUILD_TRIGGER_SA"

config :logflare, Logflare.Repo,
  username: "postgres",
  password: "postgres",
  database: "logflare_test",
  hostname: "localhost",
  pool_size: 10,
  pool: Ecto.Adapters.SQL.Sandbox

config :logflare, :postgres_backend_adapter, pool_size: 3
config :grpc, start_server: false
