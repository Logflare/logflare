use Mix.Config

config :logflare,
  node_shutdown_code: "d1032129-500c-4ab4-bcc9-853665509b6b",
  env: :dev

config :logflare, LogflareWeb.Endpoint,
  server: true,
  http: [
    port: System.get_env("PORT") || 4000,
    transport_options: [
      max_connections: 16_384,
      num_acceptors: 100,
      socket_opts: [{:raw, 0xFFFF, 0x0200, <<1::32-native>>}]
    ],
    protocol_options: [max_keepalive: 1_000],
    compress: true
  ],
  live_view: [
    signing_salt: "eVpFFmpN+OHPrilThexLilWnF+a8zBLbCtdH/OzAayShcm1B3OHOyGiadM6qOezp"
  ],
  debug_errors: true,
  code_reloader: true,
  check_origin: false,
  watchers: [
    # build js files
    npm: ["run", "watch", cd: Path.expand("../assets", __DIR__)]
  ],
  live_reload: [
    patterns: [
      ~r{priv/static/.*(js|css|png|jpeg|jpg|gif|svg)$},
      ~r{priv/gettext/.*(po)$},
      ~r{lib/logflare_web/views/.*(ex)$},
      ~r{lib/logflare_web/templates/.*(eex)$},
      ~r{lib/logflare_web/live/.*(ex)$}
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
  install_vercel_uri: "https://vercel.com/integrations/logflare-v2-dev/new"

config :logflare, Logflare.Cluster.Utils, min_cluster_size: 1

config :logflare, LogflareWeb.Plugs.PartnerAuthentication,
  token: "ydqr8TatrwEq/x/LMbhX1iUgl50MrdJ7cgXtzT+5s7KWG0bGt9d2DWio7dqqHtys"

config :open_api_spex, :cache_adapter, OpenApiSpex.Plug.NoneCache

config :stripity_stripe,
  api_key: "sk_test_thisisaboguskey",
  api_base_url: "http://localhost:12111/v1/"

config :logflare, Logflare.Vault,
  ciphers: [
    default:
      {Cloak.Ciphers.AES.GCM,
       tag: "AES.GCM.V1", key: "Sy1NN20+lvHCnE6t6JV5MSp+q1SCrOH82dCOdfwszIo=" |> Base.decode64!()}
  ]

import_config("telemetry.exs")
