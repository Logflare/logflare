# This file is responsible for configuring your application
# and its dependencies with the aid of the Mix.Config module.
#
# This configuration file is loaded before any dependency and
# is restricted to this project.
import Config

# General application configuration
config :logflare,
  ecto_repos: [Logflare.Repo],
  # https://cloud.google.com/compute/docs/instances/deleting-instance#delete_timeout
  # preemtible is 30 seconds from shutdown to sigterm
  # normal instances can be more than 90 seconds
  sigterm_shutdown_grace_period_ms: 15_000

config :logflare, Logflare.Google, dataset_id_append: "_default"

# Configures the endpoint
config :logflare, LogflareWeb.Endpoint,
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
  optional_scopes: ~w(read write),
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

config :logflare, Logflare.CacheBuster,
  replication_slot: :temporary,
  publications: ["logflare_pub"],
  # remember to add an ALTER PUBLICATION ... migration when changing published tables!
  publication_tables: [
    "billing_accounts",
    "plans",
    "rules",
    "source_schemas",
    "sources",
    "users"
  ]

config :open_api_spex, :cache_adapter, OpenApiSpex.Plug.PersistentTermCache

config :logflare, Logflare.Cluster.Utils, min_cluster_size: 1

config :grpc, start_server: true

config :logflare, Logflare.AlertsScheduler, init_task: {Logflare.Alerting, :init_alert_jobs, []}

import_config "#{Mix.env()}.exs"
