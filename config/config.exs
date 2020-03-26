# This file is responsible for configuring your application
# and its dependencies with the aid of the Mix.Config module.
#
# This configuration file is loaded before any dependency and
# is restricted to this project.
use Mix.Config

# General application configuration
config :logflare,
  ecto_repos: [Logflare.Repo]

# Configures the endpoint
config :logflare, LogflareWeb.Endpoint,
  url: [host: "localhost"],
  secret_key_base: "DSzZYeAgGaXlfRXPQqMOPiA8hJOYSImhnR2lO8lREOE2vWDmkGn1XWHxoCZoASlP",
  render_errors: [view: LogflareWeb.ErrorView, accepts: ~w(html json)],
  pubsub: [name: Logflare.PubSub]

# Configures Elixir's Logger
config :logger, :console,
  handle_otp_reports: true,
  handle_sasl_reports: true

config :ueberauth, Ueberauth,
  providers: [
    github: {Ueberauth.Strategy.Github, [default_scope: "user:email"]},
    google: {Ueberauth.Strategy.Google, []},
    slack: {Ueberauth.Strategy.Slack, [default_scope: "identity.basic,identity.email"]}
  ],
  json_library: Jason

# Use Jason for JSON parsing in Phoenix
config :phoenix, :json_library, Jason

config :logflare, ExOauth2Provider,
  repo: Logflare.Repo,
  resource_owner: Logflare.User,
  grant_flows: ~w(authorization_code),
  use_refresh_token: true,
  default_scopes: ~w(public),
  optional_scopes: ~w(read write),
  revoke_refresh_token_on_use: true,
  otp_app: :logflare,
  access_token_expires_in: nil

config :logflare, PhoenixOauth2Provider,
  current_resource_owner: :user,
  web_module: LogflareWeb,
  force_ssl_in_redirect_uri: true

config :logflare, Logflare.Mailer,
  adapter: Swoosh.Adapters.Mailgun,
  domain: "logflare.app"

config :swoosh, local: false

config :tesla,
  adapter:
    {Tesla.Adapter.Hackney, [pool: Client.BigQuery, max_connections: 50, recv_timeout: 60_000]}

config :number,
  delimit: [
    precision: 0,
    delimiter: ",",
    separator: "."
  ]

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
config :logflare, LogflareWeb.Endpoint,
  live_view: [
    signing_salt: System.get_env("PHOENIX_LIVE_VIEW_SECRET_SALT")
  ]

config :scrivener_html,
  routes_helper: LogflareWeb.Router.Helpers,
  # If you use a single view style everywhere, you can configure it here. See View Styles below for more info.
  view_style: :bootstrap_v4

config :logflare,
  sigterm_shutdown_grace_period_ms: 1_000

import_config "#{Mix.env()}.exs"
