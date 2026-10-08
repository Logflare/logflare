defmodule IngestQA.Verify do
  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Backends.Adaptor.PostgresAdaptor.PgRepo
  alias Logflare.Backends.Backend
  alias Logflare.Backends.SourceSup
  alias Logflare.Repo
  alias Logflare.Rules.Rule
  alias Logflare.SingleTenant
  alias Logflare.Sources

  def run(config, run_id) do
    user = SingleTenant.get_default_user()
    main = Sources.get_by(name: config.main, user_id: user.id)
    default_backend = SingleTenant.get_default_backend()

    checks =
      [
        {"SourceSup started for #{config.main}", Backends.source_sup_started?(main)}
        | for target <- config.targets do
            case target.type do
              :source ->
                sink = Sources.get_by(name: target.name, user_id: user.id)
                {"SourceSup started for #{target.name}", Backends.source_sup_started?(sink)}

              :backend ->
                drain = Repo.get_by(Backend, name: target.name, user_id: user.id)
                rule = Repo.get_by(Rule, source_id: main.id, backend_id: drain.id)
                {"rule child started for #{target.name}", SourceSup.rule_child_started?(rule)}
            end
          end
      ]

    stores =
      [{config.main, default_backend, main, config.kinds}] ++
        for target <- config.targets do
          case target.type do
            :source ->
              {target.name, default_backend, Sources.get_by(name: target.name, user_id: user.id),
               target.expect}

            :backend ->
              {target.name, drain(user, target.name), main, target.expect}
          end
        end

    expected =
      Map.new(stores, fn {name, _, _, kinds} ->
        {name, Enum.sort(for k <- kinds, c <- config.channels, do: "#{k} from #{c} #{run_id}")}
      end)

    actual = await(stores, run_id, expected, 30)

    results =
      checks ++
        for {name, _, _, _} <- stores do
          {"#{name} gets exactly #{inspect(expected[name] |> Enum.map(&hd(String.split(&1))) |> Enum.frequencies())}",
           actual[name] == expected[name]}
        end

    for {label, ok?} <- results,
        do: IO.puts("QA_CHECK #{if ok?, do: "PASS", else: "FAIL"} #{label}")

    for {name, _, _, _} <- stores, actual[name] != expected[name] do
      IO.puts(
        "QA_DIFF #{name} missing=#{inspect(expected[name] -- actual[name])} unexpected=#{inspect(actual[name] -- expected[name])}"
      )
    end

    IO.puts("QA_RESULT #{if Enum.all?(results, &elem(&1, 1)), do: "PASS", else: "FAIL"}")
  end

  defp await(stores, run_id, expected, attempts) do
    actual =
      Map.new(stores, fn {name, backend, source, _} ->
        {name, messages(backend, source, run_id)}
      end)

    if actual == expected or attempts == 0 do
      actual
    else
      Process.sleep(1_000)
      await(stores, run_id, expected, attempts - 1)
    end
  end

  defp drain(user, name),
    do: Backends.get_backend(Repo.get_by(Backend, name: name, user_id: user.id).id)

  defp messages(backend, source, run_id) do
    table =
      case backend.config[:schema] do
        nil -> PgRepo.table_name(source)
        schema -> "#{schema}.#{PgRepo.table_name(source)}"
      end

    sql =
      "select body->>'event_message' as message from #{table} where body->>'event_message' like $1"

    case PostgresAdaptor.execute_query(backend, {sql, ["% #{run_id}"]}, []) do
      {:ok, %{rows: rows}} -> rows |> Enum.map(& &1["message"]) |> Enum.sort()
      {:error, _} -> []
    end
  end
end

{config, _} = Code.eval_file("{{SKILL_DIR}}/scripts/targets.exs")
IngestQA.Verify.run(config, "{{RUN_ID}}")
