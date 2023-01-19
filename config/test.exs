use Mix.Config

# We don't run a server during test. If one is required,
# you can enable the server option below.
config :logflare, LogflareWeb.Endpoint,
  http: [port: 4001],
  server: false

config :logflare, env: :test

config :logger, :console, metadata: :all, level: :error

config :logflare, Logflare.Google,
  dataset_id_append: "_test",
  project_number: "1023172132421",
  project_id: "logflare-dev-238720",
  service_account: "logflare-dev@logflare-dev-238720.iam.gserviceaccount.com"

config :logflare, Logflare.Tracker, pool_size: 5, level: :error

config :logflare, Logflare.Repo,
  username: "postgres",
  password: "postgres",
  database: "logflare_test",
  hostname: "localhost",
  pool_size: 10,
  pool: Ecto.Adapters.SQL.Sandbox
