import Config

if config_env() == :prod do
  config :logflare, LogflareWeb.Endpoint,
    url: [
      host: System.get_env("PHX_URL_HOST"),
      scheme: System.get_env("PHX_URL_SCHEME"),
      port: String.to_integer(System.get_env("PHX_URL_PORT"))
    ],
    check_origin: String.split(System.get_env("PHX_CHECK_ORIGIN", ""), ","),
    live_view: [signing_salt: System.get_env("PHX_LIVE_VIEW_SIGNING_SALT")]

  config :logflare, Logflare.Repo,
    pool_size: String.to_integer(System.get_env("DB_POOL_SIZE")),
    ssl: System.get_env("DB_SSL") == "true",
    database: System.get_env("DB_DATABASE"),
    hostname: System.get_env("DB_HOSTNAME"),
    password: System.get_env("DB_PASSWORD"),
    port: 5432

  config :logflare, Logflare.Cluster.Utils,
    min_cluster_size: String.to_integer(System.get_env("LOGFLARE_CLUSTER_SIZE"))

  config :logflare_agent, url: System.get_env("LOGFLARE_AGENT_URL")

  config :logflare_logger_backend,
    source_id: System.get_env("LOGFLARE_LOGGER_BACKEND_SOURCE_ID"),
    url: System.get_env("LOGFLARE_LOGGER_BACKEND_URL"),
    api_key: System.get_env("LOGFLARE_LOGGER_BACKEND_API_KEY")

  gce_topoloty = [
    gce: [
      strategy: Logflare.Cluster.Strategy.GoogleComputeEngine,
      config: [release_name: :logflare]
    ]
  ]

  config :libcluster,
    topologies:
      if(System.get_env("LIBCLUSTER_TOPOLOGY", "gce") == "gce", do: gce_topoloty, else: [])

  config :logflare, Logflare.Cluster.Strategy.GoogleComputeEngine,
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
    redirect_uri: System.get_env("VERCEL_CLIENT_REDIRECT_URI"),
    install_vercel_uri: System.get_env("VERCEL_CLIENT_INSTALL_URI")

  config :logflare, Logflare.Google,
    dataset_id_append: System.get_env("GOOGLE_DATASET_ID_APPEND"),
    project_number: System.get_env("GOOGLE_PROJECT_NUMBER"),
    project_id: System.get_env("GOOGLE_PROJECT_ID"),
    service_account: System.get_env("GOOGLE_SERVICE_ACCOUNT"),
    compute_engine_sa: System.get_env("GOOGLE_COMPUTE_ENGINE_SA"),
    api_sa: System.get_env("GOOGLE_API_SA"),
    cloud_build_sa: System.get_env("GOOGLE_CLOUD_BUILD_SA"),
    gcp_cloud_build_sa: System.get_env("GOOGLE_GCP_CLOUD_BUILD_SA"),
    compute_system_iam_sa: System.get_env("GOOGLE_COMPUTE_SYSTEM_IAM_SA"),
    container_engine_robot_sa: System.get_env("GOOGLE_CONTAINER_ENGINE_ROBOT_SA"),
    dataproc_sa: System.get_env("GOOGLE_DATAPROC_SA"),
    redis_sa: System.get_env("GOOGLE_REDIS_SA"),
    serverless_robot_sa: System.get_env("GOOGLE_SERVERLESS_ROBOT_SA"),
    service_networking_sa: System.get_env("GOOGLE_SERVICE_NETWORKING_SA"),
    source_repo_sa: System.get_env("GOOGLE_SOURCE_REPO_SA")

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

  config :goth, json: File.read!(System.get_env("GOTH_FILE_PATH"))
end
