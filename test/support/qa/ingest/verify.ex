defmodule Logflare.QA.Ingest.Verify do
  @moduledoc """
  Checks the server node after an ingest run: supervisors and rule children are
  running, and each routing target stored exactly the events its rule matches.
  Runs on the server node and returns results for the caller to print.
  """

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Backends.Adaptor.PostgresAdaptor.PgRepo
  alias Logflare.Backends.Backend
  alias Logflare.Backends.SourceSup
  alias Logflare.QA.Check
  alias Logflare.QA.Ingest.Targets
  alias Logflare.Repo
  alias Logflare.Rules.Rule
  alias Logflare.SingleTenant
  alias Logflare.Sources

  @spec run(Targets.t(), String.t()) :: [Check.t()]
  def run(config, run_id) do
    user = SingleTenant.get_default_user()
    main = Sources.get_by(name: config.main, user_id: user.id)
    stores = stores(config, user, main)

    expected =
      Map.new(stores, fn {name, _, _, kinds} ->
        {name,
         Enum.sort(
           for kind <- kinds,
               channel <- config.channels,
               do: Targets.message(kind, channel, run_id)
         )}
      end)

    actual = await(stores, run_id, expected, 30)

    node_checks(config, user, main) ++
      for {name, _, _, kinds} <- stores do
        %Check{
          label: "#{name} gets exactly #{Enum.join(kinds, ", ")} from each channel",
          ok?: actual[name] == expected[name],
          detail:
            "missing #{inspect(expected[name] -- actual[name])}, unexpected #{inspect(actual[name] -- expected[name])}"
        }
      end
  end

  defp node_checks(config, user, main) do
    [
      %Check{
        label: "SourceSup started for #{config.main}",
        ok?: Backends.source_sup_started?(main)
      }
      | for target <- config.targets do
          case target.type do
            :source ->
              sink = Sources.get_by(name: target.name, user_id: user.id)

              %Check{
                label: "SourceSup started for #{target.name}",
                ok?: Backends.source_sup_started?(sink)
              }

            :backend ->
              rule =
                Repo.get_by(Rule, source_id: main.id, backend_id: drain(user, target.name).id)

              %Check{
                label: "rule child started for #{target.name}",
                ok?: SourceSup.rule_child_started?(rule)
              }
          end
        end
    ]
  end

  defp stores(config, user, main) do
    default_backend = SingleTenant.get_default_backend()

    [
      {config.main, default_backend, main, config.kinds}
      | for target <- config.targets do
          case target.type do
            :source ->
              {target.name, default_backend, Sources.get_by(name: target.name, user_id: user.id),
               target.expect}

            :backend ->
              {target.name, drain(user, target.name), main, target.expect}
          end
        end
    ]
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
