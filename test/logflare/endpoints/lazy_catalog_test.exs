defmodule Logflare.Endpoints.LazyCatalogTest do
  use Logflare.DataCase

  alias Logflare.Alerting
  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Backends.Adaptor.QueryResult
  alias Logflare.Endpoints

  setup do
    insert(:plan)
    user = insert(:user)
    source = insert(:source, user: user, name: "logs")
    backend = insert(:backend, user: user, type: :bigquery)

    endpoint =
      insert(:endpoint,
        user: user,
        backend: backend,
        language: :bq_sql,
        query: "SELECT 1 AS n FROM logs"
      )

    test_pid = self()

    stub(BigQueryAdaptor, :execute_query, fn _, args, _ ->
      send(test_pid, {:executed, elem(args, 0)})
      {:ok, QueryResult.new([%{"n" => 1}], %{total_bytes_processed: 0})}
    end)

    %{user: user, source: source, endpoint: endpoint}
  end

  test "constant queries skip the name index and both full catalogs", context do
    reject_full_catalogs()

    for sql <- transformed_queries(%{context.endpoint | query: "SELECT 1 AS n"}) do
      assert sql =~ "SELECT 1"
    end

    assert Cachex.size!(Endpoints.Cache) == 0
  end

  test "source-only queries skip both full catalogs and reuse the compact name index", context do
    reject_full_catalogs()

    expect(Endpoints, :list_query_names, fn user_id ->
      Mimic.call_original(Endpoints, :list_query_names, [user_id])
    end)

    for sql <- transformed_queries(context.endpoint) do
      assert sql =~ String.replace(to_string(context.source.token), "-", "_")
    end
  end

  for {language, type} <- [ch_sql: :clickhouse, pg_sql: :postgres] do
    @tag language: language, backend_type: type
    test "#{language} source-only previews skip both full catalogs", context do
      backend = insert(:backend, user: context.user, type: context.backend_type)
      reject_full_catalogs()
      endpoint = %{context.endpoint | language: context.language, backend_id: backend.id}
      assert {:ok, _} = Endpoints.get_transformed_query(endpoint)
    end
  end

  test "ClickHouse source-only executions skip both full catalogs", context do
    backend = insert(:backend, user: context.user, type: :clickhouse)
    reject_full_catalogs()
    endpoint = %{context.endpoint | language: :ch_sql, backend_id: backend.id}

    expect(ClickHouseAdaptor, :execute_query, fn _, _, _ ->
      {:ok, QueryResult.new([%{"n" => 1}], %{total_bytes_processed: 0})}
    end)

    assert {:ok, %{rows: [%{"n" => 1}]}} = Endpoints.run_query(endpoint)
  end

  test "CTEs and nested source references do not require full catalogs", context do
    reject_full_catalogs()

    query =
      "WITH recent AS (SELECT 1 AS n FROM logs) SELECT 1 AS n FROM (SELECT 1 AS n FROM recent) AS r"

    for sql <- transformed_queries(%{context.endpoint | query: query}) do
      assert sql =~ "recent"
    end
  end

  test "a different-language endpoint with the source's name does not trigger catalog loads",
       context do
    insert(:endpoint, user: context.user, name: "logs", language: :pg_sql)
    reject_full_catalogs()
    assert length(transformed_queries(context.endpoint)) == 2
  end

  test "endpoint references retain precedence over sources and alerts with the same name",
       context do
    insert(:endpoint,
      user: context.user,
      name: "logs",
      language: :bq_sql,
      query: "SELECT 'endpoint_wins' AS n"
    )

    insert(:alert,
      user: context.user,
      name: "logs",
      language: :bq_sql,
      query: "SELECT 'alert_loses' AS n"
    )

    for sql <- transformed_queries(context.endpoint) do
      assert sql =~ "endpoint_wins"
      refute sql =~ "alert_loses"
    end
  end

  test "alert references expand even when their name also belongs to a source", context do
    insert(:alert,
      user: context.user,
      name: "logs",
      language: :bq_sql,
      query: "SELECT 'alert_wins' AS n"
    )

    for sql <- transformed_queries(context.endpoint) do
      assert sql =~ "alert_wins"
    end
  end

  test "the calling endpoint is excluded but an alert with its id is not", context do
    insert(:alert,
      id: context.endpoint.id,
      user: context.user,
      name: context.endpoint.name,
      language: :bq_sql,
      query: "SELECT 'same_id_alert' AS n"
    )

    endpoint = %{context.endpoint | query: ~s(SELECT 1 AS n FROM "#{context.endpoint.name}")}

    for sql <- transformed_queries(endpoint) do
      assert sql =~ "same_id_alert"
    end
  end

  test "self references remain source references when no other named query matches", context do
    endpoint = Repo.update!(Ecto.Changeset.change(context.endpoint, name: "logs"))
    reject_full_catalogs()
    assert length(transformed_queries(endpoint)) == 2
  end

  test "quoted endpoint references inside joins and nested queries still expand", context do
    insert(:endpoint,
      user: context.user,
      name: "named.endpoint",
      language: :bq_sql,
      query: "SELECT 'nested_endpoint' AS n"
    )

    query = ~s|SELECT 1 AS n FROM logs JOIN (SELECT 1 AS n FROM "named.endpoint") AS e ON true|

    for sql <- transformed_queries(%{context.endpoint | query: query}) do
      assert sql =~ "nested_endpoint"
    end
  end

  test "CTE alias collisions retain the existing expansion behavior", context do
    insert(:endpoint,
      user: context.user,
      name: "report",
      language: :bq_sql,
      query: "SELECT 'cte_collision' AS n"
    )

    query = "WITH report AS (SELECT 1 AS n) SELECT 1 AS n FROM report"

    for sql <- transformed_queries(%{context.endpoint | query: query}) do
      assert sql =~ "cte_collision"
    end
  end

  test "another owner's matching name does not require catalogs", context do
    insert(:endpoint, name: "logs", language: :bq_sql)
    reject_full_catalogs()
    assert length(transformed_queries(context.endpoint)) == 2
  end

  test "sandbox SQL overrides retain source-only lazy expansion", context do
    reject_full_catalogs()

    endpoint = %{
      context.endpoint
      | query: "WITH recent AS (SELECT 1 AS n FROM logs) SELECT 1 AS n FROM recent",
        sandboxable: true
    }

    assert length(transformed_queries(endpoint, %{"sql" => "SELECT 1 AS n FROM recent"})) == 2
  end

  test "empty and malformed queries return errors without catalog reads", context do
    reject_full_catalogs()
    reject(Endpoints, :list_query_names, 1)

    for query <- ["", "SELECT FROM"] do
      endpoint = %{context.endpoint | query: query}
      assert {:error, _} = Endpoints.run_query(endpoint)
      assert {:error, _} = Endpoints.get_transformed_query(endpoint)
    end
  end

  test "an unavailable name index propagates its error without loading full catalogs", context do
    reject_full_catalogs()

    stub(Endpoints, :list_query_names, fn _ ->
      raise DBConnection.ConnectionError, message: "name index unavailable"
    end)

    assert {:error, :database_unavailable} = Endpoints.run_query(context.endpoint)
    assert {:error, :database_unavailable} = Endpoints.get_transformed_query(context.endpoint)
  end

  defp reject_full_catalogs do
    reject(Endpoints, :list_endpoints_by, 1)
    reject(Alerting, :list_alert_queries_by_user_id, 1)
  end

  defp transformed_queries(endpoint, params \\ %{}) do
    assert {:ok, preview} = Endpoints.get_transformed_query(endpoint, params)
    assert {:ok, %{rows: [%{"n" => 1}]}} = Endpoints.run_query(endpoint, params)
    assert_receive {:executed, executed}
    [preview, executed]
  end
end
