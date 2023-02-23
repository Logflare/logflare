import Config

filter_nil_kv_pairs = fn pairs when is_list(pairs) ->
  Enum.filter(pairs, fn {_k, v} -> v !== nil end)
end

config :logflare,
       [
         node_shutdown_code: System.get_env("LOGFLARE_NODE_SHUTDOWN_CODE"),
         recaptcha_secret: System.get_env("LOGFLARE_RECAPTCHA_SECRET"),
         config_cat_sdk_key: System.get_env("LOGFLARE_CONFIG_CAT_SDK_KEY"),
         single_tenant: System.get_env("LOGFLARE_SINGLE_TENANT"),
         supabase_mode: System.get_env("LOGFLARE_SUPABASE_MODE"),
         api_key: System.get_env("LOGFLARE_API_KEY")
       ]
       |> filter_nil_kv_pairs.()

config :logflare,
       LogflareWeb.Endpoint,
       [
         http:
           [
             port: System.get_env("PHX_HTTP_PORT")
           ]
           |> filter_nil_kv_pairs.(),
         url:
           [
             host: System.get_env("PHX_URL_HOST"),
             scheme: System.get_env("PHX_URL_SCHEME"),
             port: System.get_env("PHX_URL_PORT")
           ]
           |> filter_nil_kv_pairs.(),
         secret_key_base: System.get_env("PHX_SECRET_KEY_BASE"),
         check_origin:
           case System.get_env("PHX_CHECK_ORIGIN") do
             nil ->
               nil

             value when is_binary(value) ->
               String.split(value, ",")
           end,
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
         after_connect:
           if(System.get_env("DB_SCHEMA"),
             do: {Postgrex, :query!, ["set search_path=#{System.get_env("DB_SCHEMA")}", []]},
             else: nil
           ),
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
         min_cluster_size: System.get_env("LOGFLARE_MIN_CLUSTER_SIZE", "3") |> String.to_integer()
       ]
       |> filter_nil_kv_pairs.()

config :logflare_logger_backend,
       [
         source_id: System.get_env("LOGFLARE_LOGGER_BACKEND_SOURCE_ID"),
         url: System.get_env("LOGFLARE_LOGGER_BACKEND_URL"),
         api_key: System.get_env("LOGFLARE_LOGGER_BACKEND_API_KEY")
       ]
       |> filter_nil_kv_pairs.()

log_level =
  case String.downcase(System.get_env("LOGFLARE_LOG_LEVEL") || "") do
    "warn" -> :warn
    "info" -> :info
    "error" -> :error
    "debug" -> :debug
    _ -> nil
  end

config :logger, [level: log_level] |> filter_nil_kv_pairs.()

if System.get_env("LOGFLARE_LOGGER_BACKEND_URL") != nil do
  config :logger,
    backends: [:console, LogflareLogger.HttpBackend]
end

# Set libcluster topologies
gce_topology = [
  gce: [
    strategy: Logflare.Cluster.Strategy.GoogleComputeEngine,
    config: [release_name: :logflare]
  ]
]

config :libcluster,
  topologies: if(System.get_env("LIBCLUSTER_TOPOLOGY") == "gce", do: gce_topology, else: [])

config :logflare, Logflare.Cluster.Strategy.GoogleComputeEngine,
  regions:
    System.get_env("LIBCLUSTER_TOPOLOGY_GCE_REGIONS", "")
    |> String.split(",")
    |> Enum.reject(&(&1 == ""))
    |> Enum.map(&String.split(&1, "|"))
    |> Enum.map(&List.to_tuple/1),
  zones:
    System.get_env("LIBCLUSTER_TOPOLOGY_GCE_ZONES", "")
    |> String.split(",")
    |> Enum.reject(&(&1 == ""))
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
         client_id: System.get_env("VERCEL_CLIENT_ID"),
         client_secret: System.get_env("VERCEL_CLIENT_SECRET"),
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
         cloud_build_sa: System.get_env("GOOGLE_CLOUD_BUILD_SA"),
         cloud_build_trigger_sa: System.get_env("GOOGLE_CLOUD_BUILD_TRIGGER_SA")
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

if System.get_env("LOGFLARE_MAILER_API_KEY") do
  api_key = System.get_env("LOGFLARE_MAILER_API_KEY")
  config :logflare, Logflare.Mailer, adapter: Swoosh.Adapters.Mailgun, api_key: api_key
  config :swoosh, local: false
end

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

vault_key = System.get_env("LOGFLARE_CLOAK_VAULT_KEY")

if vault_key do
  config :logflare, Logflare.Vault,
    ciphers: [
      default: {Cloak.Ciphers.AES.GCM, tag: "AES.GCM.V1", key: Base.decode64!(vault_key)}
    ]
end
