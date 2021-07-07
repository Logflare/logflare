use Mix.Config

config :logflare, env: :dev

config :logflare, LogflareWeb.Endpoint,
  http: [
    port: System.get_env("PORT") || 4000,
    transport_options: [max_connections: 16_384, num_acceptors: 100],
    protocol_options: [max_keepalive: 1_000]
  ],
  # url: [host: "dev.chasegranberry.net", scheme: "https", port: 443],
  debug_errors: true,
  code_reloader: true,
  check_origin: false,
  watchers: [
    node: [
      "node_modules/webpack/bin/webpack.js",
      "--mode",
      "development",
      "--watch",
      cd: Path.expand("../assets", __DIR__)
    ]
  ]

config :logflare, LogflareWeb.Endpoint,
  live_reload: [
    patterns: [
      ~r{priv/static/.*(js|css|png|jpeg|jpg|gif|svg)$},
      ~r{priv/gettext/.*(po)$},
      ~r{lib/logflare_web/views/.*(ex)$},
      ~r{lib/logflare_web/templates/.*(eex)$},
      ~r{lib/logflare_web/live/.*(ex)$}
    ]
  ]

config :logger,
  level: :debug,
  backends: [:console, LogflareLogger.HttpBackend]

config :logger, :console,
  format: "\n[$level] [$metadata] $message\n",
  metadata: [:request_id]

config :phoenix, :stacktrace_depth, 20

config :logflare, Logflare.Repo,
  username: "chasegranberry",
  password: "",
  database: "logflare",
  hostname: "localhost",
  pool_size: 10,
  prepare: :unnamed,
  log: false

config :logflare, Logflare.Google,
  dataset_id_append: "_dev",
  project_number: "1023172132421",
  project_id: "logflare-dev-238720",
  service_account: "logflare-dev@logflare-dev-238720.iam.gserviceaccount.com",
  compute_engine_sa: "1023172132421-compute@developer.gserviceaccount.com",
  api_sa: "1023172132421@cloudservices.gserviceaccount.com",
  cloud_build_sa: "1023172132421@cloudbuild.gserviceaccount.com",
  gcp_cloud_build_sa: "service-1023172132421@gcp-sa-cloudbuild.iam.gserviceaccount.com",
  compute_system_iam_sa: "service-1023172132421@compute-system.iam.gserviceaccount.com",
  container_engine_robot_sa:
    "service-1023172132421@container-engine-robot.iam.gserviceaccount.com",
  dataproc_sa: "service-1023172132421@dataproc-accounts.iam.gserviceaccount.com",
  container_registry_sa: "service-1023172132421@containerregistry.iam.gserviceaccount.com",
  redis_sa: "service-1023172132421@cloud-redis.iam.gserviceaccount.com",
  serverless_robot_sa: "service-1023172132421@serverless-robot-prod.iam.gserviceaccount.com",
  service_networking_sa: "service-1023172132421@service-networking.iam.gserviceaccount.com",
  source_repo_sa: "service-1023172132421@sourcerepo-service-accounts.iam.gserviceaccount.com"

config :libcluster,
  topologies: [
    dev: [
      strategy: Cluster.Strategy.Epmd,
      config: [
        hosts: [:"orange@127.0.0.1", :"pink@127.0.0.1"]
      ],
      connect: {:net_kernel, :connect_node, []},
      disconnect: {:net_kernel, :disconnect_node, []}
    ]
  ]

config :logflare, Logflare.Tracker, pool_size: 1

config :logflare, LogflareWeb.Auth.VercelAuth,
  vercel_app_host: "https://phx.chasegranberry.net",
  client_id: "9b73d10edd067ba404148b28ef1eb4b1cb2a7027ade973b6cadc2b24f7c16702"

config :logflare, Logflare.Vercel.Client,
  client_id: "oac_mfhbqP7U20BH3IbJYmhsNNj1",
  client_secret: "AyAOgIeI4TmOgnqgUTA9u68z",
  redirect_uri: "http://localhost:4000/install/vercel-v2",
  install_vercel_uri: "https://vercel.com/integrations/logflare-dev/new"

config :goth,
  json: "config/secrets/logflare-dev-238720-63d50e3c9cc8.json" |> File.read!()

config :logflare,
  recaptcha_site_key: "6LerPsoUAAAAAMPNe7nb4dBMDDN4w6tGtmQXn8bh",
  recaptcha_secret: "6LerPsoUAAAAAM2MP18GAsePKntkjBiANAV35Z1z"

import_config "dev.secret.exs"
import_config "telemetry.exs"
