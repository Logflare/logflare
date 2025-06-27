import Config

filter_nil_kv_pairs = fn pairs when is_list(pairs) ->
  Enum.filter(pairs, fn {_k, v} -> v !== nil end)
end

logflare_metadata =
  [cluster: System.get_env("LOGFLARE_METADATA_CLUSTER")]
  |> filter_nil_kv_pairs.()

logflare_health =
  [
    memory_utilization:
      System.get_env("LOGFLARE_HEALTH_MAX_MEMORY_UTILIZATION", "0.95") |> String.to_float()
  ]
  |> filter_nil_kv_pairs.()

config :logflare,
       Logflare.PubSub,
       [
         pool_size:
           if(System.get_env("LOGFLARE_PUBSUB_POOL_SIZE") != nil,
             do: String.to_integer(System.get_env("LOGFLARE_PUBSUB_POOL_SIZE")),
             else: 56
           )
       ]
       |> filter_nil_kv_pairs.()

config :logflare,
       [
         node_shutdown_code: System.get_env("LOGFLARE_NODE_SHUTDOWN_CODE"),
         recaptcha_secret: System.get_env("LOGFLARE_RECAPTCHA_SECRET"),
         config_cat_sdk_key: System.get_env("LOGFLARE_CONFIG_CAT_SDK_KEY"),
         single_tenant: System.get_env("LOGFLARE_SINGLE_TENANT", "false") == "true",
         supabase_mode: System.get_env("LOGFLARE_SUPABASE_MODE", "false") == "true",
         public_access_token:
           System.get_env("LOGFLARE_PUBLIC_ACCESS_TOKEN") || System.get_env("LOGFLARE_API_KEY"),
         private_access_token: System.get_env("LOGFLARE_PRIVATE_ACCESS_TOKEN"),
         cache_stats: System.get_env("LOGFLARE_CACHE_STATS", "false") == "true",
         encryption_key_default: System.get_env("LOGFLARE_DB_ENCRYPTION_KEY"),
         encryption_key_retired: System.get_env("LOGFLARE_DB_ENCRYPTION_KEY_RETIRED"),
         metadata: logflare_metadata,
         health: logflare_health
       ]
       |> filter_nil_kv_pairs.()

config :logflare,
       :bigquery_backend_adaptor,
       [
         managed_service_account_pool_size:
           System.get_env("LOGFLARE_BIGQUERY_MANAGED_SA_POOL", "0")
           |> String.to_integer()
       ]
       |> filter_nil_kv_pairs.()

config :logflare,
       :bigquery_backend_adaptor,
       [
         managed_service_account_pool_size:
           System.get_env("LOGFLARE_BIGQUERY_MANAGED_SA_POOL", "0")
           |> String.to_integer()
       ]
       |> filter_nil_kv_pairs.()

config :logflare,
       Logflare.Alerting,
       [
         min_cluster_size:
           if(System.get_env("LOGFLARE_ALERTS_MIN_CLUSTER_SIZE") != nil,
             do: String.to_integer(System.get_env("LOGFLARE_ALERTS_MIN_CLUSTER_SIZE")),
             else: nil
           ),
         enabled: System.get_env("LOGFLARE_ALERTS_ENABLED", "true") == "true"
       ]
       |> filter_nil_kv_pairs.()

config :logflare,
       LogflareWeb.Endpoint,
       filter_nil_kv_pairs.(
         http: filter_nil_kv_pairs.(port: System.get_env("PHX_HTTP_PORT")),
         url:
           filter_nil_kv_pairs.(
             host: System.get_env("PHX_URL_HOST"),
             scheme: System.get_env("PHX_URL_SCHEME"),
             port: System.get_env("PHX_URL_PORT")
           ),
         secret_key_base: System.get_env("PHX_SECRET_KEY_BASE"),
         check_origin:
           case System.get_env("PHX_CHECK_ORIGIN") do
             nil -> nil
             value when is_binary(value) -> String.split(value, ",")
           end,
         live_view:
           [signing_salt: System.get_env("PHX_LIVE_VIEW_SIGNING_SALT")]
           |> filter_nil_kv_pairs.(),
         live_dashboard: System.get_env("LOGFLARE_ENABLE_LIVE_DASHBOARD", "false") == "true"
       )

config :logflare,
       Logflare.Repo,
       filter_nil_kv_pairs.(
         pool_size:
           if(System.get_env("DB_POOL_SIZE") != nil,
             do: String.to_integer(System.get_env("DB_POOL_SIZE")),
             else: nil
           ),
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
       )

if System.get_env("LOGFLARE_MIN_CLUSTER_SIZE") do
  config :logflare,
         Logflare.Cluster.Utils,
         min_cluster_size: System.get_env("LOGFLARE_MIN_CLUSTER_SIZE") |> String.to_integer()
end

config :logflare_logger_backend,
       filter_nil_kv_pairs.(
         source_id: System.get_env("LOGFLARE_LOGGER_BACKEND_SOURCE_ID"),
         url: System.get_env("LOGFLARE_LOGGER_BACKEND_URL"),
         api_key: System.get_env("LOGFLARE_LOGGER_BACKEND_API_KEY")
       )

config :logger,
  backends:
    [
      :console,
      if(System.get_env("LOGFLARE_LOGGER_BACKEND_URL") != nil,
        do: LogflareLogger.HttpBackend,
        else: nil
      ),
      if(System.get_env("LOGFLARE_LOGGER_JSON") == "true", do: LoggerJSON, else: nil)
    ]
    |> Enum.filter(&(&1 != nil))

config :logger,
  metadata: logflare_metadata

log_level =
  case String.downcase(System.get_env("LOGFLARE_LOG_LEVEL") || "") do
    # TODO: remove at v2
    "warn" -> :warning
    "warning" -> :warning
    "info" -> :info
    "debug" -> :debug
    "error" -> :error
    _ -> nil
  end

config :logger, filter_nil_kv_pairs.(level: log_level)

config :logflare,
       LogflareWeb.Auth.VercelAuth,
       filter_nil_kv_pairs.(
         vercel_app_host: System.get_env("VERCEL_AUTH_HOST"),
         client_id: System.get_env("VERCEL_AUTH_CLIENT_ID")
       )

config :logflare,
       Logflare.Vercel.Client,
       filter_nil_kv_pairs.(
         client_id: System.get_env("VERCEL_CLIENT_ID"),
         client_secret: System.get_env("VERCEL_CLIENT_SECRET"),
         redirect_uri: System.get_env("VERCEL_CLIENT_REDIRECT_URI"),
         install_vercel_uri: System.get_env("VERCEL_CLIENT_INSTALL_URI")
       )

config :logflare,
       Logflare.Google,
       filter_nil_kv_pairs.(
         dataset_id_append: System.get_env("GOOGLE_DATASET_ID_APPEND"),
         project_number: System.get_env("GOOGLE_PROJECT_NUMBER"),
         project_id: System.get_env("GOOGLE_PROJECT_ID"),
         service_account: System.get_env("GOOGLE_SERVICE_ACCOUNT"),
         compute_engine_sa: System.get_env("GOOGLE_COMPUTE_ENGINE_SA"),
         grafana_sa: System.get_env("GOOGLE_GRAFANA_SA"),
         api_sa: System.get_env("GOOGLE_API_SA"),
         cloud_build_sa: System.get_env("GOOGLE_CLOUD_BUILD_SA"),
         cloud_build_trigger_sa: System.get_env("GOOGLE_CLOUD_BUILD_TRIGGER_SA")
       )

config :ueberauth,
       Ueberauth.Strategy.Github.OAuth,
       filter_nil_kv_pairs.(
         client_id: System.get_env("UEBERAUTH_GITHUB_CLIENT_ID"),
         client_secret: System.get_env("UEBERAUTH_GITHUB_CLIENT_SECRET")
       )

config :ueberauth,
       Ueberauth.Strategy.Google.OAuth,
       filter_nil_kv_pairs.(
         client_id: System.get_env("UEBERAUTH_GOOGLE_CLIENT_ID"),
         client_secret: System.get_env("UEBERAUTH_GOOGLE_CLIENT_SECRET")
       )

config :ueberauth,
       Ueberauth.Strategy.SlackV2.OAuth,
       filter_nil_kv_pairs.(
         client_id: System.get_env("UEBERAUTH_SLACK_CLIENT_ID"),
         client_secret: System.get_env("UEBERAUTH_SLACK_CLIENT_SECRET")
       )

if System.get_env("LOGFLARE_MAILER_API_KEY") do
  api_key = System.get_env("LOGFLARE_MAILER_API_KEY")
  config :logflare, Logflare.Mailer, adapter: Swoosh.Adapters.Mailgun, api_key: api_key
  config :swoosh, local: false
end

config :ex_twilio,
       filter_nil_kv_pairs.(
         account_sid: System.get_env("TWILLIO_ACCOUNT_SID"),
         auth_token: System.get_env("TWILLIO_AUTH_TOKEN")
       )

config :stripity_stripe,
       filter_nil_kv_pairs.(
         api_key: System.get_env("STRIPE_API_KEY"),
         publishable_key: System.get_env("STRIPE_PUBLISHABLE_KEY")
       )

if config_env() != :test do
  config :grpc, port: System.get_env("LOGFLARE_GRPC_PORT", "50051") |> String.to_integer()
end

cond do
  System.get_env("LOGFLARE_SINGLE_TENANT", "false") == "true" &&
      not is_nil(System.get_env("POSTGRES_BACKEND_URL")) ->
    config :logflare,
           :postgres_backend_adapter,
           filter_nil_kv_pairs.(
             url: System.get_env("POSTGRES_BACKEND_URL"),
             schema: System.get_env("POSTGRES_BACKEND_SCHEMA")
           )

  config_env() != :test ->
    if File.exists?("gcloud.json") do
      config :goth, json: File.read!("gcloud.json")
    end

  config_env() == :test ->
    :ok

  true ->
    raise "Missing Google or Backend credentials"
end

if(
  System.get_env("LOGFLARE_ENABLE_GRPC_SSL") == "true" &&
    File.exists?("cert.pem") && File.exists?("cert.key")
) do
  config :logflare,
    ssl: [
      certfile: "cert.pem",
      keyfile: "cert.key"
    ]
end

if(
  System.get_env("DB_SSL") == "true" && File.exists?("db-server-ca.pem") &&
    File.exists?("db-client-ca.pem") && File.exists?("db-client-key.pem")
) do
  config :logflare, Logflare.Repo,
    ssl: true,
    ssl_opts: [
      #  ssl opts follow recs here: https://erlef.github.io/security-wg/secure_coding_and_deployment_hardening/ssl
      verify: :verify_peer,
      cacertfile: "db-server-ca.pem",
      certfile: "db-client-cert.pem",
      keyfile: "db-client-key.pem",
      versions: [:"tlsv1.2"],
      # support wildcard
      customize_hostname_check: [
        match_fun: :public_key.pkix_verify_hostname_match_fun(:https)
      ]
    ]
end

case System.get_env("LOGFLARE_FEATURE_FLAG_OVERRIDE") do
  nil ->
    nil

  value ->
    config :logflare,
      feature_flag_override:
        value
        |> String.downcase()
        |> String.split(",")
        |> Enum.map(&String.split(&1, "="))
        |> Enum.map(&[List.to_tuple(&1)])
        |> Enum.map(&Map.new/1)
        |> Enum.reduce(&Map.merge/2)
end

if config_env() != :test do
  config :telemetry_poller, :default,
    vm_measurements: [:memory, :total_run_queue_lengths],
    period: 1_000
end

postgres_topology = [
  postgres: [
    strategy: Logflare.Cluster.PostgresStrategy,
    config: [
      release_name: :logflare
    ]
  ]
]

config :libcluster,
  topologies:
    if(System.get_env("LIBCLUSTER_TOPOLOGY") == "postgres", do: postgres_topology, else: [])

if System.get_env("LOGFLARE_OTEL_ENDPOINT") do
  default_sample_ratio =
    System.get_env("LOGFLARE_OTEL_SAMPLE_RATIO", "1.0")
    |> String.to_float()

  ingest_sample_ratio =
    System.get_env("LOGFLARE_OTEL_INGEST_SAMPLE_RATIO")
    |> case do
      nil -> default_sample_ratio
      value -> String.to_float(value)
    end

  endpoint_sample_ratio =
    System.get_env("LOGFLARE_OTEL_ENDPOINT_SAMPLE_RATIO")
    |> case do
      nil -> default_sample_ratio
      value -> String.to_float(value)
    end

  config :logflare,
    opentelemetry_enabled?: true,
    ingest_sample_ratio: ingest_sample_ratio,
    endpoint_sample_ratio: endpoint_sample_ratio

  config :opentelemetry,
    sdk_disabled: false,
    traces_exporter: :otlp,
    sampler:
      {:parent_based,
       %{
         root:
           {LogflareWeb.OpenTelemetrySampler,
            %{
              probability:
                System.get_env("LOGFLARE_OTEL_SAMPLE_RATIO", "1.0")
                |> String.to_float()
            }}
       }}

  config :opentelemetry_exporter,
    otlp_protocol: :http_protobuf,
    otlp_endpoint: System.get_env("LOGFLARE_OTEL_ENDPOINT"),
    otlp_compression: :gzip,
    otlp_headers: [
      {"x-source", System.get_env("LOGFLARE_OTEL_SOURCE_UUID")},
      {"x-api-key", System.get_env("LOGFLARE_OTEL_ACCESS_TOKEN")}
    ]
end

syn_endpoints_partitions =
  for n <- 0..System.schedulers_online(), do: "endpoints_#{n}" |> String.to_atom()

config :syn,
  scopes: [:core, :alerting] ++ syn_endpoints_partitions,
  event_handler: Logflare.SynEventHandler
