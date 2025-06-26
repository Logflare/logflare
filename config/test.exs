import Config

# We don't run a server during test. If one is required,
# you can enable the server option below.
config :logflare, LogflareWeb.Endpoint,
  http: [port: 4001],
  server: false

config :logflare,
  env: :test,
  encryption_key_default: "Q+IS7ogkzRxsj+zAIB1u6jNFquxkFzSrBZXItN27K/Q="

config :logflare, Logflare.Cluster.Utils, min_cluster_size: 1

config :logflare, Logflare.Source.BigQuery.Schema, updates_per_minute: 900_000

config :logflare, Logflare.Google,
  dataset_id_append: "_test",
  project_number: "1023172132421",
  project_id: "logflare-dev-238720",
  service_account: "logflare-dev@logflare-dev-238720.iam.gserviceaccount.com",
  compute_engine_sa: "GOOGLE_COMPUTE_ENGINE_SA",
  grafana_sa: "GOOGLE_GRAFANA_SA",
  api_sa: "GOOGLE_API_SA",
  cloud_build_sa: "GOOGLE_CLOUD_BUILD_SA",
  cloud_build_trigger_sa: "GOOGLE_CLOUD_BUILD_TRIGGER_SA"

config :logflare, Logflare.Repo,
  username: "postgres",
  password: "postgres",
  database: "logflare_test",
  hostname: "localhost",
  port: 5432,
  pool_size: 10,
  pool: Ecto.Adapters.SQL.Sandbox

config :logflare, :postgres_backend_adapter, pool_size: 1

config :logflare, Logflare.PubSub, pool_size: 10

defmodule LogflareTest.LogFilters do
  def ignore_finch_disconnections(%{meta: %{mfa: {Finch.HTTP2.Pool, :disconnected, _}}}, _opts) do
    :stop
  end

  def ignore_finch_disconnections(le, _opts), do: le
end

config :logger,
  default_handler: [
    filters: [
      {:finch_silencer, {&LogflareTest.LogFilters.ignore_finch_disconnections/2, []}}
    ],
    level: :error
  ]

config :tesla, Logflare.Backends.Adaptor.WebhookAdaptor.Client, adapter: Tesla.Mock

config :phoenix_test, :endpoint, LogflareWeb.Endpoint
