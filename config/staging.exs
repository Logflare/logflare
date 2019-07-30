use Mix.Config

config :logflare, env: :staging

config :logflare, LogflareWeb.Endpoint,
  http: [port: 4000, transport_options: [max_connections: 16384, num_acceptors: 10]],
  url: [host: "logflarestaging.com", scheme: "https"],
  cache_static_manifest: "priv/static/cache_manifest.json",
  server: true,
  code_reloader: false,
  version: Application.spec(:logflare, :vsn)

config :logger, :console, format: "[$level] $message\n"

config :logger,
  level: :debug

config :phoenix, :serve_endpoints, true

config :logflare, Logflare.Repo,
  pool_size: 15,
  ssl: true,
  prepare: :unnamed,
  timeout: 30_000

config :logflare, Logflare.Google,
  dataset_id_append: "_staging",
  project_number: "395392434060",
  project_id: "logflare-staging",
  service_account: "logflare-staging@logflare-staging.iam.gserviceaccount.com"

config :logflare_logger_backend,
  api_key: "aaaaa",
  source_id: "bbbbbb",
  flush_interval: 1_000,
  max_batch_size: 50,
  url: "http://example.com"

config :logflare_agent,
  sources: [
    %{
      path: "/home/logflare/app_release/logflare/var/log/erlang.log.1",
      source: "06709b0b-a5de-4cda-a31b-3dedcd71bc5d"
    },
    %{
      path: "/home/logflare/app_release/logflare/var/log/erlang.log.2",
      source: "06709b0b-a5de-4cda-a31b-3dedcd71bc5d"
    },
    %{
      path: "/home/logflare/app_release/logflare/var/log/erlang.log.3",
      source: "06709b0b-a5de-4cda-a31b-3dedcd71bc5d"
    },
    %{
      path: "/home/logflare/app_release/logflare/var/log/erlang.log.4",
      source: "06709b0b-a5de-4cda-a31b-3dedcd71bc5d"
    },
    %{
      path: "/home/logflare/app_release/logflare/var/log/erlang.log.5",
      source: "06709b0b-a5de-4cda-a31b-3dedcd71bc5d"
    }
  ],
  url: "https://api.logflare.app"

import_config "staging.secret.exs"
