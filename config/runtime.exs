import Config

logger_config =
  case System.get_env("LOGGER_CONFIG", config_env() |> Atom.to_string()) do
    "prod" ->
      [
        level: :info,
        backends: [LogflareLogger.HttpBackend],
        sync_threshold: 10_001,
        discard_threshold: 10_000,
        compile_time_purge_matching: [
          [level_lower_than: :info]
        ]
      ]

    "test" ->
      [level: :error, backends: [:console]]

    _ ->
      [level: :info, backends: [:console], metadata: :all]
  end

config :logger, logger_config

if config_env() != :test do
  if config_env() == :prod do
    config :logflare_agent,
      api_key: System.get_env("LOGFLARE_AGENT_API_KEY"),
      url: System.get_env("LOGFLARE_AGENT_URL")
  end

  config :logflare,
    node_shutdown_code: System.get_env("LOGFLARE_NODE_SHUTDOWN_CODE"),
    recaptcha_secret: System.get_env("LOGFLARE_RECAPTCHA_SECRET")

  config :logflare, LogflareWeb.Endpoint,
    url: [
      host: System.get_env("PHX_URL_HOST", "127.0.0.1"),
      scheme: System.get_env("PHX_URL_SCHEME", "http"),
      port: String.to_integer(System.get_env("PHX_URL_PORT", "4000"))
    ],
    secret_key_base:
      System.get_env(
        "PHX_SECRET_KEY_BASE",
        "xyP317ErRnpx3khZqnj3kUMMFdC1dMD+G292U1HfhM9y01gE1R64TO3A/ur6mBg3"
      ),
    check_origin: String.split(System.get_env("PHX_CHECK_ORIGIN", ""), ","),
    live_view: [
      signing_salt:
        System.get_env(
          "PHX_LIVE_VIEW_SIGNING_SALT",
          "oVsImHxKuwhVI93xygKts96zKOjkNhb7vIcrsxT/4BTyIrNp3duNZ/Nj7SGv0GzX"
        )
    ]

  config :logflare, Logflare.Repo,
    pool_size: String.to_integer(System.get_env("DB_POOL_SIZE", "10")),
    ssl: System.get_env("DB_SSL") == "true",
    database: System.get_env("DB_DATABASE", "logflare"),
    hostname: System.get_env("DB_HOSTNAME", "localhost"),
    password: System.get_env("DB_PASSWORD", "postgres"),
    username: System.get_env("DB_USERNAME", "postgres"),
    port: String.to_integer(System.get_env("DB_PORT", "5432"))

  config :logflare, Logflare.Cluster.Utils,
    min_cluster_size: String.to_integer(System.get_env("LOGFLARE_CLUSTER_SIZE", "10"))

  config :logflare_logger_backend,
    source_id: System.get_env("LOGFLARE_LOGGER_BACKEND_SOURCE_ID"),
    url: System.get_env("LOGFLARE_LOGGER_BACKEND_URL", "http://127.0.0.1:4000"),
    api_key: System.get_env("LOGFLARE_LOGGER_BACKEND_API_KEY")

  gce_topoloty = [
    gce: [
      strategy: Logflare.Cluster.Strategy.GoogleComputeEngine,
      config: [release_name: :logflare]
    ]
  ]

  config :libcluster,
    topologies: if(System.get_env("LIBCLUSTER_TOPOLOGY") == "gce", do: gce_topoloty, else: [])

  config :logflare, Logflare.Cluster.Strategy.GoogleComputeEngine,
    regions:
      System.get_env("LIBCLUSTER_TOPOLOGY_GCE_REGIONS", "")
      |> String.split(",")
      |> Enum.map(&String.split(&1, "|"))
      |> Enum.map(&List.to_tuple/1),
    zones:
      System.get_env("LIBCLUSTER_TOPOLOGY_GCE_ZONES", "")
      |> String.split(",")
      |> Enum.map(&String.split(&1, "|"))
      |> Enum.map(&List.to_tuple/1)

  config :logflare, LogflareWeb.Auth.VercelAuth,
    vercel_app_host: System.get_env("VERCEL_AUTH_HOST"),
    client_id: System.get_env("VERCEL_AUTH_CLIENT_ID")

  config :logflare, Logflare.Vercel.Client,
    client_id: System.get_env("VERCEL_CLIENT_CLIENT_ID"),
    client_secret: System.get_env("VERCEL_CLIENT_CLIENT_SECRET"),
    redirect_uri: System.get_env("VERCEL_CLIENT_REDIRECT_URI"),
    install_vercel_uri: System.get_env("VERCEL_CLIENT_INSTALL_URI")

  config :logflare, Logflare.Google,
    dataset_id_append: System.get_env("GOOGLE_DATASET_ID_APPEND"),
    project_number: System.get_env("GOOGLE_PROJECT_NUMBER"),
    project_id: System.get_env("GOOGLE_PROJECT_ID"),
    service_account: System.get_env("GOOGLE_SERVICE_ACCOUNT"),
    compute_engine_sa: System.get_env("GOOGLE_COMPUTE_ENGINE_SA"),
    api_sa: System.get_env("GOOGLE_API_SA"),
    cloud_build_sa: System.get_env("GOOGLE_CLOUD_BUILD_SA")

  config :ueberauth, Ueberauth.Strategy.Github.OAuth,
    client_id: System.get_env("UEBERAUTH_GITHUB_CLIENT_ID"),
    client_secret: System.get_env("UEBERAUTH_GITHUB_CLIENT_SECRET")

  config :ueberauth, Ueberauth.Strategy.Google.OAuth,
    client_id: System.get_env("UEBERAUTH_GOOGLE_CLIENT_ID"),
    client_secret: System.get_env("UEBERAUTH_GOOGLE_CLIENT_SECRET")

  config :ueberauth, Ueberauth.Strategy.SlackV2.OAuth,
    client_id: System.get_env("UEBERAUTH_SLACK_CLIENT_ID"),
    client_secret: System.get_env("UEBERAUTH_SLACK_CLIENT_SECRET")

  config :logflare, Logflare.Mailer, api_key: System.get_env("LOGFLARE_MAILER_API_KEY")

  config :ex_twilio,
    account_sid: System.get_env("TWILLIO_ACCOUNT_SID"),
    auth_token: System.get_env("TWILLIO_AUTH_TOKEN")

  config :stripity_stripe,
    api_key: System.get_env("STRIPE_API_KEY"),
    publishable_key: System.get_env("STRIPE_PUBLISHABLE_KEY")

  config :goth, json: File.read!("gcloud.json")
end
