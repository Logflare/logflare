defmodule Logflare.Mixfile do
  @moduledoc false
  use Mix.Project
  @version "1.0.0"

  def project do
    [
      app: :logflare,
      version: @version,
      elixir: "~> 1.4",
      elixirc_paths: elixirc_paths(Mix.env()),
      compilers: [:phoenix, :gettext] ++ Mix.compilers(),
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      deps: deps(),
      preferred_cli_env: [
        lint: :test,
        "lint.diff": :test,
        "test.format": :test,
        "test.compile": :test,
        "test.security": :test,
        "test.typings": :test,
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test
      ],
      test_coverage: [tool: ExCoveralls],
      releases: [
        logflare: [
          version: @version,
          include_executables_for: [:unix],
          applications: [runtime_tools: :permanent, ssl: :permanent]
        ]
      ]
    ]
  end

  # Configuration for the OTP application.
  #
  # Type `mix help compile.app` for more information.
  def application do
    [
      mod: {Logflare.Application, []},
      extra_applications: [
        :logger,
        :runtime_tools,
        :ueberauth_github,
        :ueberauth_google,
        :ssl,
        :phoenix_html,
        :phoenix,
        :erlexec
      ],
      included_applications: [:mnesia]
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib", "priv/tasks"]

  defp deps do
    [
      # Phoenix and LogflareWeb
      {:phoenix, "~> 1.5.0", override: true},
      {:phoenix_pubsub, "~> 2.0.0"},
      {:phoenix_ecto, "~> 4.4"},
      {:phoenix_html, "~> 2.10"},
      {:phoenix_live_reload, "~> 1.0", only: :dev},
      # {:plug, "~> 1.8"},
      {:plug_cowboy, "~> 2.0"},
      {:plug_crypto, "~> 1.2.2"},
      {:phoenix_live_view, "~> 0.15.3", override: true},
      {:phoenix_live_dashboard, "~> 0.3.0"},
      {:cors_plug, "~> 2.0"},

      # Oauth
      {:ueberauth_google, "~> 0.8"},
      {:ueberauth_github, github: "Logflare/ueberauth_github"},
      {:ueberauth_slack_v2, "~> 1.0"},
      {:oauth2, "~> 2.0.0", override: true},

      # Oauth2 provider
      {:phoenix_oauth2_provider, "~> 0.5.1"},
      {:ex_oauth2_provider, github: "aristamd/ex_oauth2_provider", override: true},

      # Ecto and DB
      {:postgrex, ">= 0.0.0"},
      {:gettext, "~> 0.11"},
      {:jason, "~> 1.0"},
      {:deep_merge, "~> 1.0"},
      {:number, "~> 1.0.0"},
      {:timex, "~> 3.1"},
      {:typed_struct, "~> 0.1"},
      {:lqueue, "~> 1.1"},
      {:cachex, "~> 3.1"},
      {:ex_machina, "~> 2.3"},
      {:iteraptor, "~> 1.10"},
      {:decorator, "~> 1.3"},
      {:atomic_map, "~> 0.9.3"},
      {:libcluster, "~> 3.2"},
      {:map_keys, "~> 0.1.0"},
      {:observer_cli, "~> 1.5"},

      # Parsing
      {:bertex, ">= 0.0.0"},
      {:nimble_parsec, "~> 1.0"},
      {:warpath, "~> 0.5.0"},
      {:timber_logfmt, github: "Logflare/logfmt-elixir"},

      # Outbound Requests
      {:castore, "~> 0.1.0"},
      {:finch, "~> 0.11.0"},
      {:mint, "~> 1.0"},
      # {:hackney, github: "benoitc/hackney", override: true},
      {:httpoison, "~> 1.4"},
      {:poison, "~> 5.0.0", override: true},
      {:swoosh, "~> 0.23"},
      {:ex_twilio, "~> 0.8.1"},
      {:tesla, "~> 1.4.0"},

      # Concurrency and pipelines
      {:broadway, "~> 1.0.3"},
      {:flow, "~> 1.0"},
      {:nimble_options, "~>0.4.0"},

      # Test
      {:mix_test_watch, "~> 1.0", only: [:dev, :test], runtime: false},
      {:faker, "~> 0.12", only: :test},
      {:mimic, "~> 1.0", only: :test},

      # Pagination
      {:scrivener_ecto, "~> 2.2"},
      {:scrivener_list, "~> 2.0"},
      {:scrivener_html, "~> 1.8"},

      # GCP
      {:google_api_cloud_resource_manager, "~> 0.34.0"},
      {:google_api_big_query, "~> 0.52.0"},
      {:goth, "~> 1.3-rc"},

      # Ecto
      {:ecto, "~> 3.9", override: true},
      {:ecto_sql, "~> 3.9"},
      {:typed_ecto_schema, "~> 0.1.0"},

      # Telemetry & logging
      {:telemetry, "~> 0.4.0"},
      {:telemetry_poller, "0.5.0"},
      {:telemetry_metrics, "~> 0.6.0", override: true},
      {:logflare_logger_backend, "~> 0.10.2"},
      {:logflare_agent, "~> 0.6.2", only: [:prod]},

      # ETS
      {:ets, "~> 0.8.0"},
      {:ex2ms, "~> 1.0"},
      {:etso, "~> 0.1.1"},

      # Statistics
      {:statistex, "~> 1.0.0"},

      # HTML
      {:floki, "~> 0.29.0"},

      # Rust NIFs
      {:rustler, "~> 0.21.0", override: true},

      # Frontend
      {:phoenix_live_react, "~> 0.4"},

      # Dev
      {:dialyxir, "~> 1.1", only: [:dev, :test], runtime: false},

      # Billing
      {:stripity_stripe, "~> 2.9.0"},
      {:money, "~> 1.7"},

      # Utils
      {:recase, "~> 0.7.0"},
      {:ex_unicode, "~> 1.0"},
      {:assertions, "~> 0.19.0", only: [:test]},

      # Code quality
      {:credo, "~> 1.6", only: [:dev, :test], runtime: false},
      {:sobelow, "~> 0.11", only: [:dev, :test], runtime: false},
      {:excoveralls, "~> 0.10", only: :test},

      # Telemetry
      # {:logflare_telemetry, github: "Logflare/logflare_telemetry_ex", only: :dev}

      # Charting
      {:contex, "~> 0.3.0"},

      # SQL
      {:erlexec, "~>1.18.11"},

      # Postgres Subscribe
      {:cainophile, "~> 0.1.0"}
      # {:honeydew, "~> 1.5.0"}
    ]
  end

  defp aliases do
    [
      setup: ["deps.get", "cmd elixir --sname orange --cookie monster -S mix ecto.setup"],
      start: "cmd PORT=4000 iex --sname orange --cookie monster -S mix phx.server",
      "start.orange":
        "cmd PORT=4000 iex --name orange@127.0.0.1 --cookie monster -S mix phx.server",
      "start.pink": "cmd PORT=4001 iex --name pink@127.0.0.1 --cookie monster -S mix phx.server",
      # coveralls will trigger unit tests as well
      test: ["ecto.create --quiet", "ecto.migrate", "test"],
      "test.compile": ["compile --warnings-as-errors"],
      "test.format": ["format --check-formatted"],
      "test.security": ["sobelow --threshold high"],
      "test.typings": ["dialyzer --format short"],
      "test.coverage": ["coveralls"],
      lint: ["credo"],
      "lint.diff": ["credo diff master"],
      "lint.all": ["credo --strict"],
      "ecto.setup": ["ecto.create", "ecto.migrate", "run priv/repo/seeds.exs"],
      "ecto.reset": ["ecto.drop", "ecto.setup"]
    ]
  end
end
