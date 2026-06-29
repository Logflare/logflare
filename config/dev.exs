import Config

config :logflare,
  env: :dev,
  dev_routes: true,
  node_shutdown_code: "d1032129-500c-4ab4-bcc9-853665509b6b"

config :logflare, LogflareWeb.Endpoint,
  server: true,
  http: [
    port: System.get_env("PORT") || 4000
  ],
  live_view: [
    signing_salt: "eVpFFmpN+OHPrilThexLilWnF+a8zBLbCtdH/OzAayShcm1B3OHOyGiadM6qOezp"
  ],
  debug_errors: true,
  code_reloader: true,
  check_origin: false,
  watchers: [
    # build js files
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
      ~r{lib/logflare_web/live/.*(ex)$},
      ~r{lib/logflare_web/.*(ex)$}
    ]
  ]

config :logger, :console,
  format: "\n[$level] [$metadata] $message\n",
  metadata: [:request_id],
  level: :debug

config :phoenix, :stacktrace_depth, 20

config :logflare, Logflare.Repo,
  username: "postgres",
  password: "postgres",
  database: "logflare_dev",
  hostname: "localhost",
  port: 5432,
  pool_size: 10,
  prepare: :unnamed,
  log: false

config :logflare, Logflare.Google,
  dataset_id_append: "_dev",
  project_number: "1023172132421",
  project_id: "logflare-dev-238720",
  service_account: "logflare-dev@logflare-dev-238720.iam.gserviceaccount.com",
  compute_engine_sa: "compute-engine-2022@logflare-dev-238720.iam.gserviceaccount.com",
  api_sa: "1023172132421@cloudservices.gserviceaccount.com",
  cloud_build_sa: "1023172132421@cloudbuild.gserviceaccount.com",
  cloud_build_trigger_sa: "cloud-build@logflare-dev-238720.iam.gserviceaccount.com"

config :libcluster, debug: true

config :logflare, LogflareWeb.Auth.VercelAuth,
  vercel_app_host: "https://phx.chasegranberry.net",
  client_id: "9b73d10edd067ba404148b28ef1eb4b1cb2a7027ade973b6cadc2b24f7c16702"

config :logflare, Logflare.Vercel.Client,
  client_id: "oac_mfhbqP7U20BH3IbJYmhsNNj1",
  client_secret: "AyAOgIeI4TmOgnqgUTA9u68z",
  redirect_uri: "http://localhost:4000/install/vercel-v2",
  install_vercel_uri: "https://vercel.com/integrations/logflare-v2-dev/new"

config :logflare, Logflare.Cluster.Utils, min_cluster_size: 1

config :open_api_spex, :cache_adapter, OpenApiSpex.Plug.NoneCache

config :stripity_stripe,
  api_key: "sk_test_thisisaboguskey"

config :logflare, :s3_spool,
  mode: :disable,
  bucket: "logflare-spool",
  partitions: 4,
  batch_timeout: 5_000,
  compress: false,
  # Serialization format for spool files. Options: :ndjson | :etf
  # :etf encodes the whole batch as a single Erlang term — ~10x faster decode,
  # but files are binary (use IEx to inspect, not cat/jq).
  format: :etf,
  queue_name: "logflare-spool"

config :ex_aws,
  access_key_id: "minioadmin",
  secret_access_key: "minioadmin",
  region: "us-east-1",
  s3: [
    scheme: "http://",
    host: "localhost",
    port: 9002
  ],
  sqs: [
    scheme: "http://",
    host: "localhost",
    port: 9324
  ]

# GCP local emulators (docker-compose.gcp.yml)
# Switch to GCP by setting provider: :gcp and queue_name to the Pub/Sub topic/subscription path.
# fake-gcs-server runs on :4443, Pub/Sub emulator on :8085.
config :google_api_storage, base_url: "http://localhost:4443/"
config :google_api_pub_sub, base_url: "http://localhost:8085/"
