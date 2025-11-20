# This file is responsible for configuring your application
# and its dependencies with the aid of the Mix.Config module.
#
# This configuration file is loaded before any dependency and
# is restricted to this project.
import Config

# General application configuration

hardcoded_encryption_key = "Q+IS7ogkzRxsj+zAIB1u6jNFquxkFzSrBZXItN27K/Q="

config :logflare,
  ecto_repos: [Logflare.Repo],
  # https://cloud.google.com/compute/docs/instances/deleting-instance#delete_timeout
  # preemtible is 30 seconds from shutdown to sigterm
  # normal instances can be more than 90 seconds
  sigterm_shutdown_grace_period_ms: 15_000,
  encryption_key_fallback: hardcoded_encryption_key,
  encryption_key_default: hardcoded_encryption_key

config :logflare, Logflare.Alerting, min_cluster_size: 1, enabled: true

config :logflare, Logflare.Google, dataset_id_append: "_default"

config :logflare, :postgres_backend_adapter, pool_size: 3

config :logflare, :bigquery_backend_adaptor, managed_service_account_pool_size: 0

config :logflare, :clickhouse_backend_adaptor,
  engine: "MergeTree",
  pool_size: 3

config :logflare, Logflare.Sources.Source.BigQuery.Schema, updates_per_minute: 6

# Configures the endpoint
config :logflare, LogflareWeb.Endpoint,
  adapter: Bandit.PhoenixAdapter,
  http: [
    http_options: [log_protocol_errors: :short, log_client_closures: false],
    http_1_options: [gc_every_n_keepalive_requests: 3],
    thousand_island_options: [
      num_acceptors: 1250,
      # default backend keepalive timeout is fixed at 600 seconds
      # https://cloud.google.com/load-balancing/docs/https/request-distribution#timeout-keepalive-backends
      read_timeout: 620_000,
      # transport options are passed wholly to :gen_tcp
      # https://github.com/mtrudel/thousand_island/blob/ae733332892b1bb2482a9cf4e97de03411fba2ad/lib/thousand_island/transports/tcp.ex#L61
      transport_options: [
        # https://www.erlang.org/doc/man/inet
        # both reuseport and reuseport_lb should be provided for linux
        reuseport: true,
        reuseport_lb: true,
        # keepalive defaults to false
        keepalive: true
      ]
    ]
  ],
  url: [host: "localhost", scheme: "http", port: 4000],
  secret_key_base: "DSzZYeAgGaXlfRXPQqMOPiA8hJOYSImhnR2lO8lREOE2vWDmkGn1XWHxoCZoASlP",
  render_errors: [view: LogflareWeb.ErrorView, accepts: ~w(html json)],
  pubsub_server: Logflare.PubSub,
  live_view: [signing_salt: "Fvo_-oQi4bjPfQLh"]

config :logflare, Logflare.PubSub, pool_size: 10

# Configures Elixir's Logger
config :logger,
  handle_otp_reports: true,
  handle_sasl_reports: false,
  level: :info

config :logger_json, :backend,
  metadata: :all,
  json_encoder: Jason,
  formatter: LoggerJSON.Formatters.GoogleCloudLogger

config :ueberauth, Ueberauth,
  providers: [
    github: {Ueberauth.Strategy.Github, [default_scope: "user:email"]},
    google: {Ueberauth.Strategy.Google, []},
    slack:
      {Ueberauth.Strategy.SlackV2,
       [
         default_scope: "app_mentions:read",
         default_user_scope: "identity.basic,identity.avatar,identity.email,identity.team"
       ]}
  ],
  json_library: Jason

config :phoenix, :json_library, Jason
config :postgrex, :json_library, Jason

oauth_common = [
  repo: Logflare.Repo,
  grant_flows: ~w(authorization_code),
  use_refresh_token: true,
  default_scopes: ~w(public),
  optional_scopes: ~w(read write private),
  revoke_refresh_token_on_use: true,
  otp_app: :logflare,
  access_token_expires_in: nil
]

config :logflare, ExOauth2Provider, [resource_owner: Logflare.User] ++ oauth_common

config :logflare,
       ExOauth2ProviderPartner,
       [
         resource_owner: Logflare.Partners.Partner,
         application: Logflare.OauthApplications.PartnerOauthApplication,
         access_token: Logflare.OauthAccessTokens.PartnerOauthAccessToken,
         default_scopes: ~w(partner)
       ] ++ oauth_common

config :logflare, PhoenixOauth2Provider,
  current_resource_owner: :user,
  web_module: LogflareWeb,
  force_ssl_in_redirect_uri: true

config :logflare, Logflare.Mailer,
  adapter: Swoosh.Adapters.Local,
  domain: "logflare.app"

config :swoosh, local: true

# use the default querying connection pool
# see application.ex for pool settings
config :tesla,
  # TODO: `use Tesla.Builder` and `use Tesla` are soft-deprecated. It will be removed in future major version in favor of Runtime Configuration instead. See https://github.com/elixir-tesla/tesla/discussions/732 to learn more.
  disable_deprecated_builder_warning: true,
  adapter: {Tesla.Adapter.Finch, name: Logflare.FinchDefault, receive_timeout: 60_000}

config :number,
  delimit: [
    precision: 0,
    delimiter: ",",
    separator: "."
  ]

config :scrivener_html,
  routes_helper: LogflareWeb.Router.Helpers,
  # If you use a single view style everywhere, you can configure it here. See View Styles below for more info.
  view_style: :bootstrap_v4

config :logflare, Logflare.ContextCache.CacheBuster,
  replication_slot: :temporary,
  publications: ["logflare_pub"]

config :open_api_spex, :cache_adapter, OpenApiSpex.Plug.PersistentTermCache

config :logflare, Logflare.Cluster.Utils, min_cluster_size: 1

config :logflare, Logflare.Alerting.AlertsScheduler,
  init_task: {Logflare.Alerting, :init_alert_jobs, []}

config :logflare, Logflare.Scheduler,
  run_strategy: Quantum.RunStrategy.Local,
  jobs: [
    source_cleanup: [
      schedule: "*/15 * * * *",
      task: {Logflare.Sources, :shutdown_idle_sources, []}
    ]
  ]

config :opentelemetry,
  sdk_disabled: true,
  span_processor: :batch,
  traces_exporter: :none

config :logflare, Logflare.Vault, json_library: Jason

config :broadway, config_storage: :ets

config :mime, :types, %{
  "application/x-protobuf" => ["protobuf"]
}

# use legacy artifacts for users on older CPUs or virtualized environments without advanced CPU features
config :explorer, use_legacy_artifacts: true

import_config "#{Mix.env()}.exs"
