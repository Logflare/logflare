defmodule IngestQA.Setup do
  alias Logflare.Backends
  alias Logflare.Backends.Backend
  alias Logflare.Lql
  alias Logflare.Repo
  alias Logflare.Rules
  alias Logflare.Rules.Rule
  alias Logflare.SingleTenant
  alias Logflare.Sources
  alias Logflare.Sources.Source.BigQuery.SchemaBuilder

  def run(config) do
    user = SingleTenant.get_default_user()
    main = source(user, config.main)

    for target <- config.targets do
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

    Cachex.clear(Logflare.Rules.Cache)
    Cachex.clear(Logflare.Backends.Cache)

    for %{type: :source, name: name} <- config.targets,
        do: Backends.ensure_source_sup_started(source(user, name))

    Backends.ensure_source_sup_started(main)

    IO.puts("QA_ENV MAIN_TOKEN=#{main.token} MAIN_ID=#{main.id}")
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

{config, _} = Code.eval_file("{{SCRIPT_DIR}}/targets.exs")
IngestQA.Setup.run(config)
