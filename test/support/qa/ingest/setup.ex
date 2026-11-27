defmodule Logflare.QA.Ingest.Setup do
  @moduledoc """
  Runs on the server node. Creates the QA source, its sink sources, its Postgres drain
  backends and their rules, then starts their supervisors. Safe to run again.
  """

  alias Logflare.Backends
  alias Logflare.Backends.Backend
  alias Logflare.Lql
  alias Logflare.QA.Ingest.Targets
  alias Logflare.Repo
  alias Logflare.Rules
  alias Logflare.Rules.Rule
  alias Logflare.SingleTenant
  alias Logflare.Sources
  alias Logflare.Sources.Source.BigQuery.SchemaBuilder

  @spec run() :: %{token: String.t(), id: pos_integer()}
  def run do
    user = SingleTenant.get_default_user()
    main = source(user, Targets.main())

    for {type, name, lql, _kinds} <- Targets.targets() do
      {:ok, filters} = Lql.Parser.parse(lql, SchemaBuilder.initial_table_schema())
      attrs = %{source_id: main.id, lql_string: lql, lql_filters: filters}

      {key, value} =
        case type do
          :source -> {:sink, source(user, name).token}
          :backend -> {:backend_id, backend(user, name).id}
        end

      case Repo.get_by(Rule, [{:source_id, main.id}, {key, value}]) do
        nil -> {:ok, _} = Rules.create_rule(Map.put(attrs, key, value))
        rule -> {:ok, _} = Rules.update_rule(rule, attrs)
      end

      if type == :source, do: Backends.ensure_source_sup_started(source(user, name))
    end

    Cachex.clear(Logflare.Rules.Cache)
    Cachex.clear(Logflare.Backends.Cache)
    Backends.ensure_source_sup_started(main)

    %{token: Atom.to_string(main.token), id: main.id}
  end

  defp source(user, name) do
    with nil <- Sources.get_by(name: name, user_id: user.id) do
      {:ok, source} = Sources.create_source(%{"name" => name}, user)
      source
    end
  end

  defp backend(user, name) do
    with nil <- Repo.get_by(Backend, name: name, user_id: user.id) do
      config = %{url: System.fetch_env!("POSTGRES_BACKEND_URL"), schema: name}

      {:ok, backend} =
        Backends.create_backend(user, %{name: name, type: :postgres, config: config})

      backend
    end
  end
end
