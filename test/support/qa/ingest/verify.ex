defmodule Logflare.QA.Ingest.Verify do
  @moduledoc """
  Runs on the server node. Checks that the main source stored every event of a run and
  that each routing target stored exactly the events its rule matches.
  """

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Backends.Adaptor.PostgresAdaptor.PgRepo
  alias Logflare.Backends.Backend
  alias Logflare.QA.Ingest.Targets
  alias Logflare.Repo
  alias Logflare.SingleTenant
  alias Logflare.Sources

  @spec run(String.t()) :: [{String.t(), boolean(), String.t()}]
  def run(run_id) do
    user = SingleTenant.get_default_user()
    main = Sources.get_by(name: Targets.main(), user_id: user.id)
    default = SingleTenant.get_default_backend()

    stores = [
      {Targets.main(), default, main, Targets.kinds()}
      | for {type, name, _lql, kinds} <- Targets.targets() do
          case type do
            :source -> {name, default, Sources.get_by(name: name, user_id: user.id), kinds}
            :backend -> {name, drain(user, name), main, kinds}
          end
        end
    ]

    expected =
      Map.new(stores, fn {name, _, _, kinds} ->
        {name, Enum.sort(Targets.messages(run_id, kinds))}
      end)

    actual = await(stores, run_id, expected, 30)

    for {name, _, _, kinds} <- stores do
      {"#{name} stores exactly #{Enum.join(kinds, ", ")} from each channel",
       actual[name] == expected[name],
       "missing #{inspect(expected[name] -- actual[name])}, unexpected #{inspect(actual[name] -- expected[name])}"}
    end
  end

  defp await(stores, run_id, expected, attempts) do
    actual =
      Map.new(stores, fn {name, backend, source, _} -> {name, stored(backend, source, run_id)} end)

    if actual == expected or attempts == 0 do
      actual
    else
      Process.sleep(1_000)
      await(stores, run_id, expected, attempts - 1)
    end
  end

  defp drain(user, name),
    do: Backends.get_backend(Repo.get_by(Backend, name: name, user_id: user.id).id)

  defp stored(backend, source, run_id) do
    table =
      Enum.join(Enum.reject([backend.config[:schema], PgRepo.table_name(source)], &is_nil/1), ".")

    sql =
      "select body->>'event_message' as message from #{table} where body->>'event_message' like $1"

    case PostgresAdaptor.execute_query(backend, {sql, ["% #{run_id}"]}, []) do
      {:ok, %{rows: rows}} -> rows |> Enum.map(& &1["message"]) |> Enum.sort()
      {:error, _} -> []
    end
  end
end
