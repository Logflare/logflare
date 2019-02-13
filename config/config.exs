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
  pubsub: [name: Logflare.PubSub, adapter: Phoenix.PubSub.PG2]

# Configures Elixir's Logger
config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:user_id]

config :ueberauth, Ueberauth,
  providers: [
    github: {Ueberauth.Strategy.Github, [default_scope: "user:email,public_repo"]}
  ]

# Use Jason for JSON parsing in Phoenix
config :phoenix, :json_library, Jason

config :phoenix_oauth2_provider, PhoenixOauth2Provider,
  current_resource_owner: :user,
  repo: Logflare.Repo,
  resource_owner: Logflare.User,
  grant_flows: ~w(authorization_code),
  use_refresh_token: true,
  default_scopes: ~w(public),
  optional_scopes: ~w(read write),
  revoke_refresh_token_on_use: true

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{Mix.env()}.exs"

