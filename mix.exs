defmodule Logflare.Mixfile do
  @moduledoc false
  use Mix.Project

  def project do
    [
      app: :logflare,
      version: "0.15.1",
      elixir: "~> 1.4",
      elixirc_paths: elixirc_paths(Mix.env()),
      compilers: [:phoenix, :gettext] ++ Mix.compilers(),
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      deps: deps(),
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test
      ],
      releases: [
        logflare: [
          version: "0.14.0",
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
        :edeliver,
        :ueberauth_google,
        :ssl,
        :ueberauth_slack
      ]
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib", "priv/tasks"]

  # Specifies your project dependencies.
  #
  # Type `mix help deps` for examples and options.
  defp deps do
    [
      {:phoenix, "~> 1.4.0"},
      {:phoenix_pubsub, "~> 1.1.2"},
      {:phoenix_ecto, "~> 4.0"},
      {:postgrex, ">= 0.0.0"},
      {:phoenix_html, "~> 2.10"},
      {:phoenix_live_reload, "~> 1.0", only: :dev},
      {:gettext, "~> 0.11"},
      {:plug_cowboy, "~> 2.0"},
      {:ueberauth_github, "~> 0.7"},
      {:plug, "~> 1.8"},
      {:jason, "~> 1.0"},
      {:distillery, "~> 2.1.0"},
      {:edeliver, ">= 1.7.0"},
      {:httpoison, "~> 1.4"},
      {:phoenix_oauth2_provider, "~> 0.5.1"},
      {:ex_oauth2_provider, github: "danschultzer/ex_oauth2_provider", override: true},
      {:poison, "~> 3.1"},
      {:ueberauth_google, "~> 0.8"},
      {:swoosh, "~> 0.23"},
      {:ex_twilio, "~> 0.8.1"},
      {:google_api_big_query, "~> 0.30.0"},
      {:goth, "~> 1.2.0"},
      {:broadway, "~> 0.4.0"},
      {:google_api_cloud_resource_manager, "~> 0.5"},
      {:deep_merge, "~> 1.0"},
      {:mix_test_watch, "~> 1.0", only: :dev, runtime: false},
      {:credo, "~> 1.1", only: [:dev, :test], runtime: false},
      {:number, "~> 1.0.0"},
      {:timex, "~> 3.1"},
      {:mox, "~> 0.5", only: :test},
      {:typed_struct, "~> 0.1"},
      {:publicist, "~> 1.1.0"},
      {:lqueue, "~> 1.1"},
      {:cachex, "~> 3.1"},
      {:faker, "~> 0.12", only: :test},
      {:ex_machina, "~> 2.3"},
      {:iteraptor, "~> 1.9.0"},
      {:bertex, ">= 0.0.0"},
      {:excoveralls, "~> 0.11", only: :test, runtime: false},
      {:placebo, "~> 1.2"},
      {:logflare_logger_backend, "~> 0.6.3"},
      {:logflare_agent, "~> 0.6.2", only: [:prod]},
      {:phoenix_live_view, "0.4.1"},
      {:decorator, "~> 1.3"},
      {:atomic_map, "~> 0.9.3"},
      {:nimble_parsec, "~> 0.5.0"},
      {:scrivener_ecto, "~> 2.2.0"},
      {:scrivener_list, "~> 2.0.1"},
      {:scrivener_html, "~> 1.8.1"},
      {:libcluster, "~> 3.1.1"},
      {:map_keys, "~> 0.1.0"},
      {:tesla, "~> 1.3.0"},
      {:ueberauth_slack, "~> 0.6"},
      {:oauth2, "~> 2.0.0", override: true},

      # Ecto
      {:ecto_sql, "~> 3.2"},

      # Telemetry
      {:telemetry, "~> 0.4.0"},
      {:telemetry_poller, "~> 0.4.0"},
      {:telemetry_metrics, "~> 0.3.0"},

      # ETS
      {:ets, "~> 0.8.0"},
      {:ex2ms, "~> 1.0"},
      {:etso, "~> 0.1.1"},

      # Statistics
      {:statistex, "~> 1.0.0"},
      {:floki, "~> 0.23.1"},

      # Frontend
      {:phoenix_live_react, "~> 0.2"},

      # Dev
      {:dialyxir, "~> 1.0.0-rc.6", only: [:dev], runtime: false}
    ]
  end

  # Aliases are shortcuts or tasks specific to the current project.
  # For example, to create, migrate and run the seeds file at once:
  #
  #     $ mix ecto.setup
  #
  # See the documentation for `Mix` for more info on aliases.
  defp aliases do
    [
      "ecto.setup": ["ecto.create", "ecto.migrate", "run priv/repo/seeds.exs"],
      "ecto.reset": ["ecto.drop", "ecto.setup"]
      # compile: ["compile --warnings-as-errors"]
      # test: ["ecto.create --quiet", "ecto.migrate", "test"]
    ]
  end
end
