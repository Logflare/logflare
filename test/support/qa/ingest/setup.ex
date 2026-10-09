defmodule Logflare.QA.Ingest.Setup do
  @moduledoc """
  Creates the QA source, its sink sources, its Postgres drain backends and their rules,
  then starts their supervisors. Runs on the server node and is idempotent.
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

  @spec run(Targets.t()) :: %{token: String.t(), id: pos_integer()}
  def run(config) do
    user = SingleTenant.get_default_user()
    main = source(user, config.main)

    Enum.each(config.targets, &put_rule(user, main, &1))

    Cachex.clear(Logflare.Rules.Cache)
    Cachex.clear(Logflare.Backends.Cache)

    for %{type: :source, name: name} <- config.targets,
        do: Backends.ensure_source_sup_started(source(user, name))

    Backends.ensure_source_sup_started(main)

    %{token: Atom.to_string(main.token), id: main.id}
  end

  defp put_rule(user, main, target) do
    {:ok, filters} = Lql.Parser.parse(target.lql, SchemaBuilder.initial_table_schema())
    attrs = %{source_id: main.id, lql_string: target.lql, lql_filters: filters}

    {rule, attrs} =
      case target.type do
        :source ->
          sink = source(user, target.name)

          {Repo.get_by(Rule, source_id: main.id, sink: sink.token),
           Map.put(attrs, :sink, sink.token)}

        :backend ->
          drain = backend(user, target.name)

          {Repo.get_by(Rule, source_id: main.id, backend_id: drain.id),
           Map.put(attrs, :backend_id, drain.id)}
      end

    {:ok, _} = if rule, do: Rules.update_rule(rule, attrs), else: Rules.create_rule(attrs)
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
