use Mix.Config

config :logflare, env: :prod

config :logflare, LogflareWeb.Endpoint,
  http: [port: 4000, transport_options: [max_connections: 16384, num_acceptors: 100]],
  url: [host: "logflare.app", scheme: "https"],
  cache_static_manifest: "priv/static/cache_manifest.json",
  server: true,
  code_reloader: false,
  version: Application.spec(:logflare, :vsn)

config :logger,
  level: :info,
  backends: [LogflareLogger.HttpBackend]

config :phoenix, :serve_endpoints, true

config :logflare, Logflare.Repo,
  pool_size: 15,
  ssl: true,
  prepare: :unnamed,
  timeout: 30_000,
  queue_target: 5_000

config :logflare, Logflare.Google,
  dataset_id_append: "_prod",
  project_number: "1074203751359",
  project_id: "logflare-232118",
  service_account: "logflare@logflare-232118.iam.gserviceaccount.com"

config :logflare_agent,
  sources: [
    %{
      path: "/home/logflare/app_release/logflare/var/log/erlang.log.1",
      source: "4ec9216e-a8e9-46eb-92cb-1576092c9e4b"
    },
    %{
      path: "/home/logflare/app_release/logflare/var/log/erlang.log.2",
      source: "4ec9216e-a8e9-46eb-92cb-1576092c9e4b"
    },
    %{
      path: "/home/logflare/app_release/logflare/var/log/erlang.log.3",
      source: "4ec9216e-a8e9-46eb-92cb-1576092c9e4b"
    },
    %{
      path: "/home/logflare/app_release/logflare/var/log/erlang.log.4",
      source: "4ec9216e-a8e9-46eb-92cb-1576092c9e4b"
    },
    %{
      path: "/home/logflare/app_release/logflare/var/log/erlang.log.5",
      source: "4ec9216e-a8e9-46eb-92cb-1576092c9e4b"
    }
  ],
  url: "https://api.logflarestaging.com"

import_config "prod.secret.exs"
