import Config

filter_nil_kv_pairs = fn pairs when is_list(pairs) ->
  Enum.filter(pairs, fn {_k, v} -> v !== nil end)
end

if config_env() == :prod do
  config :logflare_agent,
         [
           api_key: System.get_env("LOGFLARE_AGENT_API_KEY"),
           url: System.get_env("LOGFLARE_AGENT_URL")
         ]
         |> filter_nil_kv_pairs.()
end

config :logflare,
       [
         node_shutdown_code: System.get_env("LOGFLARE_NODE_SHUTDOWN_CODE"),
         recaptcha_secret: System.get_env("LOGFLARE_RECAPTCHA_SECRET")
       ]
       |> filter_nil_kv_pairs.()

config :logflare,
       LogflareWeb.Endpoint,
       [
         url:
           [
             host: System.get_env("PHX_URL_HOST", "127.0.0.1"),
             scheme: System.get_env("PHX_URL_SCHEME", "http"),
             port:
               if(System.get_env("PHX_URL_PORT") != nil,
                 do: String.to_integer(System.get_env("PHX_URL_PORT")),
                 else: nil
               )
           ]
           |> filter_nil_kv_pairs.(),
         secret_key_base:
           System.get_env(
             "PHX_SECRET_KEY_BASE",
             "xyP317ErRnpx3khZqnj3kUMMFdC1dMD+G292U1HfhM9y01gE1R64TO3A/ur6mBg3"
           ),
         check_origin: String.split(System.get_env("PHX_CHECK_ORIGIN", ""), ","),
         live_view:
           [
             signing_salt: System.get_env("PHX_LIVE_VIEW_SIGNING_SALT")
           ]
           |> filter_nil_kv_pairs.()
       ]
       |> filter_nil_kv_pairs.()

config :logflare,
       Logflare.Repo,
       [
         pool_size:
           if(System.get_env("DB_POOL_SIZE") != nil,
             do: String.to_integer(System.get_env("DB_POOL_SIZE")),
             else: nil
           ),
         ssl: System.get_env("DB_SSL") == "true",
         database: System.get_env("DB_DATABASE"),
         hostname: System.get_env("DB_HOSTNAME"),
         password: System.get_env("DB_PASSWORD"),
         username: System.get_env("DB_USERNAME"),
         port:
           if(System.get_env("DB_PORT") != nil,
             do: String.to_integer(System.get_env("DB_PORT")),
             else: nil
           )
       ]
       |> filter_nil_kv_pairs.()

config :logflare,
       Logflare.Cluster.Utils,
       [
         min_cluster_size:
           if(System.get_env("LOGFLARE_CLUSTER_SIZE") != nil,
             do: String.to_integer(System.get_env("LOGFLARE_CLUSTER_SIZE")),
             else: nil
           )
       ]
       |> filter_nil_kv_pairs.()

config :logflare_logger_backend,
       [
         source_id: System.get_env("LOGFLARE_LOGGER_BACKEND_SOURCE_ID"),
         url: System.get_env("LOGFLARE_LOGGER_BACKEND_URL"),
         api_key: System.get_env("LOGFLARE_LOGGER_BACKEND_API_KEY")
       ]
       |> filter_nil_kv_pairs.()

# Set libcluster topologies
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

config :logflare,
       LogflareWeb.Auth.VercelAuth,
       [
         vercel_app_host: System.get_env("VERCEL_AUTH_HOST"),
         client_id: System.get_env("VERCEL_AUTH_CLIENT_ID")
       ]
       |> filter_nil_kv_pairs.()

config :logflare,
       Logflare.Vercel.Client,
       [
         client_id: System.get_env("VERCEL_CLIENT_CLIENT_ID"),
         client_secret: System.get_env("VERCEL_CLIENT_CLIENT_SECRET"),
         redirect_uri: System.get_env("VERCEL_CLIENT_REDIRECT_URI"),
         install_vercel_uri: System.get_env("VERCEL_CLIENT_INSTALL_URI")
       ]
       |> filter_nil_kv_pairs.()

config :logflare,
       Logflare.Google,
       [
         dataset_id_append: System.get_env("GOOGLE_DATASET_ID_APPEND"),
         project_number: System.get_env("GOOGLE_PROJECT_NUMBER"),
         project_id: System.get_env("GOOGLE_PROJECT_ID"),
         service_account: System.get_env("GOOGLE_SERVICE_ACCOUNT"),
         compute_engine_sa: System.get_env("GOOGLE_COMPUTE_ENGINE_SA"),
         api_sa: System.get_env("GOOGLE_API_SA"),
         cloud_build_sa: System.get_env("GOOGLE_CLOUD_BUILD_SA")
       ]
       |> filter_nil_kv_pairs.()

config :ueberauth,
       Ueberauth.Strategy.Github.OAuth,
       [
         client_id: System.get_env("UEBERAUTH_GITHUB_CLIENT_ID"),
         client_secret: System.get_env("UEBERAUTH_GITHUB_CLIENT_SECRET")
       ]
       |> filter_nil_kv_pairs.()

config :ueberauth,
       Ueberauth.Strategy.Google.OAuth,
       [
         client_id: System.get_env("UEBERAUTH_GOOGLE_CLIENT_ID"),
         client_secret: System.get_env("UEBERAUTH_GOOGLE_CLIENT_SECRET")
       ]
       |> filter_nil_kv_pairs.()

config :ueberauth,
       Ueberauth.Strategy.SlackV2.OAuth,
       [
         client_id: System.get_env("UEBERAUTH_SLACK_CLIENT_ID"),
         client_secret: System.get_env("UEBERAUTH_SLACK_CLIENT_SECRET")
       ]
       |> filter_nil_kv_pairs.()

config :logflare,
       Logflare.Mailer,
       [api_key: System.get_env("LOGFLARE_MAILER_API_KEY")] |> filter_nil_kv_pairs.()

config :ex_twilio,
       [
         account_sid: System.get_env("TWILLIO_ACCOUNT_SID"),
         auth_token: System.get_env("TWILLIO_AUTH_TOKEN")
       ]
       |> filter_nil_kv_pairs.()

config :stripity_stripe,
       [
         api_key: System.get_env("STRIPE_API_KEY"),
         publishable_key: System.get_env("STRIPE_PUBLISHABLE_KEY")
       ]
       |> filter_nil_kv_pairs.()

if config_env() != :test do
  config :goth, json: File.read!("gcloud.json")
end
