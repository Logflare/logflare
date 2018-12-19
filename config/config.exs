# This file is responsible for configuring your application
# and its dependencies with the aid of the Mix.Config module.
#
# This configuration file is loaded before any dependency and
# is restricted to this project.
use Mix.Config

# General application configuration
config :logtail,
  ecto_repos: [Logtail.Repo]

# Configures the endpoint
config :logtail, LogtailWeb.Endpoint,
  url: [host: "localhost"],
  secret_key_base: "DSzZYeAgGaXlfRXPQqMOPiA8hJOYSImhnR2lO8lREOE2vWDmkGn1XWHxoCZoASlP",
  render_errors: [view: LogtailWeb.ErrorView, accepts: ~w(html json)],
  pubsub: [name: Logtail.PubSub,
           adapter: Phoenix.PubSub.PG2]

# Configures Elixir's Logger
config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:user_id]

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{Mix.env}.exs"

config :ueberauth, Ueberauth,
  providers: [
    github: { Ueberauth.Strategy.Github, [default_scope: "user,user:email,public_repo"] }
  ]

config :ueberauth, Ueberauth.Strategy.Github.OAuth,
  client_id: "56b252e1df74f4e3b59c",
  client_secret: "2a92ee668fdbb81c63d61330c2dd7afdc2f343d4"
