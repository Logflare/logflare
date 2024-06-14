defmodule Logflare.Mixfile do
  @moduledoc false
  use Mix.Project

  def project do
    [
      app: :logflare,
      version: version(),
      elixir: "~> 1.4",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      deps: deps(),
      preferred_cli_env: [
        lint: :test,
        "lint.diff": :test,
        "test.only": :test,
        "test.format": :test,
        "test.compile": :test,
        "test.security": :test,
        "test.typings": :test,
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test
      ],
      dialyzer: [
        plt_local_path: "dialyzer",
        plt_core_path: "dialyzer"
      ],
      test_coverage: [tool: ExCoveralls],
      releases: [
        logflare: [
          version: version(),
          include_executables_for: [:unix],
          applications: [
            runtime_tools: :permanent,
            ssl: :permanent,
            opentelemetry_exporter: :permanent,
            opentelemetry: :temporary
          ]
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
        :crypto,
        :os_mon
      ]
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib", "priv/tasks"]

  defp deps do
    [
      # Phoenix stuff
      {:phoenix, "~> 1.7"},
      {:phoenix_live_view, "~> 0.18"},
      {:phoenix_view, "~> 2.0"},
      {:phoenix_pubsub, "~> 2.1"},
      {:phoenix_ecto, "~> 4.4"},
      {:phoenix_live_reload, "~> 1.4", only: :dev},
      {:bandit, ">= 0.7.7"},
      {:plug_crypto, "~> 1.2.2"},
      {:cors_plug, "~> 2.0"},
      {:plug_caisson, "~> 0.2.1"},

      # Oauth
      {:ueberauth_google, "~> 0.8"},
      {:ueberauth_github, github: "Logflare/ueberauth_github"},
      {:ueberauth_slack_v2, "~> 1.0"},
      {:oauth2, "~> 2.0.0", override: true},

      # Oauth2 provider
      {:phoenix_oauth2_provider,
       github: "Logflare/phoenix_oauth2_provider", ref: "36e757920e54bffa0f603bf985aff91bb8b28f2d"},
      {:ex_oauth2_provider, github: "aristamd/ex_oauth2_provider", override: true},

      # Ecto and DB
      {:postgrex, ">= 0.0.0"},
      {:gettext, "~> 0.11"},
      {:jason, "~> 1.0"},
      {:deep_merge, "~> 1.0"},
      {:number, "~> 1.0.0"},
      {:timex, "~> 3.1"},
      {:typed_struct, "~> 0.1", runtime: false},
      {:lqueue, "~> 1.1"},
      {:cachex, "~> 3.1"},
      {:ex_machina, "~> 2.3", only: [:test]},
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

      # in-app docs
      {:earmark, "~> 1.4.33"},

      # Outbound Requests
      {:castore, "~> 0.1.0"},
      {:finch, "~> 0.16.0"},
      {:mint, "~> 1.0"},
      {:httpoison, "~> 1.4"},
      {:poison, "~> 5.0.0", override: true},
      {:swoosh, "~> 0.23"},
      {:ex_twilio, "~> 0.8.1"},
      {:tesla, "~> 1.6"},

      # Concurrency and pipelines
      {:broadway, "~> 1.0.6"},
      {:syn, "~> 3.3"},

      # Test
      {:mix_test_watch, "~> 1.0", only: [:dev, :test], runtime: false},
      {:mimic, "~> 1.0", only: :test},
      {:stream_data, "~> 0.6.0", only: [:dev, :test]},

      # Pagination
      {:scrivener_ecto, "~> 2.2"},
      {:scrivener_list, "~> 2.0"},
      {:scrivener_html, github: "Logflare/scrivener_html"},

      # GCP
      {:google_api_cloud_resource_manager, "~> 0.34.0"},
      {:google_api_big_query, "~> 0.79.0"},
      {:goth, "~> 1.4.0"},
      {:google_gax, github: "Logflare/elixir-google-gax", override: true},

      # Ecto
      {:ecto, "~> 3.9", override: true},
      {:ecto_sql, "~> 3.9"},
      {:typed_ecto_schema, "~> 0.1.0", runtime: false},

      # Telemetry & logging
      {:telemetry, "~> 1.0"},
      {:telemetry_metrics, "~> 1.0"},
      {:telemetry_poller, "~> 1.1"},
      {:logflare_logger_backend, "~> 0.11.4"},
      {:logger_json, "~> 5.1"},

      # ETS
      {:ets, "~> 0.8.0"},

      # HTML
      {:floki, "~> 0.29.0", only: [:test]},

      # Rust NIFs
      {:rustler, "~> 0.29.0"},

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
      {:configcat, "~> 2.0.0"},

      # Code quality
      {:credo, "~> 1.6", only: [:dev, :test], runtime: false},
      {:sobelow, "~> 0.11", only: [:dev, :test], runtime: false},
      {:excoveralls, "~> 0.10", only: :test},

      # Charting
      {:contex, "~> 0.3.0"},

      # Postgres Subscribe
      {:cainophile, github: "Logflare/cainophile"},
      {:open_api_spex, "~> 3.16"},
      {:grpc, "~> 0.5.0"},
      {:protobuf, "~> 0.10"},
      {:gun, "~> 2.0", override: true},
      {:cowlib, ">=2.12.0", override: true},
      {:phoenix_live_dashboard, "~> 0.8"},
      {:plug_cowboy, "~> 2.0"},

      # alerts feature
      {:citrine, "~> 0.1.0"},
      {:crontab, "~> 1.1"},

      # benchmarking
      {:benchee, "~> 1.0", only: [:dev, :test]},
      {:benchee_async, "~> 0.1.0", only: [:dev, :test]},
      # Filesystem fix to respect `CFLAGS` and `LDFLAGS`
      # https://github.com/falood/file_system/pull/87
      #
      # Credo is currently holding us back
      {:file_system, "~> 1.0", override: true, only: [:dev, :test]},

      # otel
      {:opentelemetry, "~> 1.3"},
      {:opentelemetry_api, "~> 1.2"},
      {:opentelemetry_exporter, "~> 1.6"},
      {:opentelemetry_phoenix, "~> 1.1"},
      {:opentelemetry_cowboy, "~> 0.2"}
    ]
  end

  defp aliases do
    [
      setup: ["deps.get", "ecto.setup", "ecto.seed"],
      # coveralls will trigger unit tests as well
      test: ["ecto.create --quiet", "ecto.migrate", "test --no-start"],
      "test.only": ["test --no-start"],
      "test.watch": ["test.watch --no-start"],
      "test.compile": ["compile --warnings-as-errors"],
      "test.format": ["format --check-formatted"],
      "test.security": ["sobelow --threshold high --ignore Config.HTTPS"],
      "test.typings": ["cmd mkdir -p dialyzer", "dialyzer"],
      "test.coverage": ["coveralls"],
      lint: ["credo"],
      "lint.diff": ["credo diff master"],
      "lint.all": ["credo --strict"],
      "ecto.seed": ["run priv/repo/seeds.exs"],
      "ecto.setup": ["ecto.create", "ecto.migrate"],
      "ecto.reset": ["ecto.drop", "ecto.setup"]
    ]
  end

  defp version, do: File.read!(Path.join(__DIR__, "VERSION"))
end
