defmodule Logflare.Alerting.LazyCatalogTest do
  use Logflare.DataCase

  alias Logflare.Alerting
  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Backends.Adaptor.QueryResult
  alias Logflare.Endpoints

  setup do
    insert(:plan)
    user = insert(:user)
    source = insert(:source, user: user, name: "logs")
    alert = insert(:alert, user: user, language: :bq_sql, query: "SELECT 1 AS n FROM logs")
    test_pid = self()

    stub(BigQueryAdaptor, :execute_query, fn _, {sql, []}, _ ->
      send(test_pid, {:executed, sql})
      {:ok, QueryResult.new([%{"n" => 1}], %{total_bytes_processed: 0})}
    end)

    %{user: user, source: source, alert: alert}
  end

  test "constant alerts skip the name index and both full catalogs", context do
    reject_full_catalogs()
    reject(Endpoints, :list_query_names, 1)

    assert execute(%{context.alert | query: "SELECT 1 AS n"}) =~ "SELECT 1"
  end

  test "source-only alerts reuse the compact name index without loading full catalogs", context do
    reject_full_catalogs()

    expect(Endpoints, :list_query_names, fn user_id ->
      Mimic.call_original(Endpoints, :list_query_names, [user_id])
    end)

    for _ <- 1..2 do
      assert execute(context.alert) =~ String.replace(to_string(context.source.token), "-", "_")
    end
  end

  test "CTEs and nested source references skip full catalogs", context do
    reject_full_catalogs()

    query =
      "WITH recent AS (SELECT 1 AS n FROM logs) SELECT 1 AS n FROM (SELECT 1 AS n FROM recent) AS r"

    assert execute(%{context.alert | query: query}) =~ "recent"
  end

  test "other owners and languages do not trigger full catalog reads", context do
    insert(:endpoint, name: "logs", language: :bq_sql)
    insert(:endpoint, user: context.user, name: "logs", language: :pg_sql)
    insert(:alert, user: context.user, name: "logs", language: :pg_sql)
    reject_full_catalogs()

    assert execute(context.alert) =~ String.replace(to_string(context.source.token), "-", "_")
  end

  test "endpoint references retain precedence over sources and alerts", context do
    insert(:endpoint,
      user: context.user,
      name: "logs",
      query: "SELECT 'endpoint_wins' AS n"
    )

    insert(:alert, user: context.user, name: "logs", query: "SELECT 'alert_loses' AS n")

    sql = execute(context.alert)
    assert sql =~ "endpoint_wins"
    refute sql =~ "alert_loses"
  end

  test "alert references expand even when their name also belongs to a source", context do
    insert(:alert, user: context.user, name: "logs", query: "SELECT 'alert_wins' AS n")

    assert execute(context.alert) =~ "alert_wins"
  end

  test "the calling alert is excluded but an endpoint with its id is not", context do
    insert(:endpoint,
      id: context.alert.id,
      user: context.user,
      name: "logs",
      query: "SELECT 'same_id_endpoint' AS n"
    )

    assert execute(context.alert) =~ "same_id_endpoint"
  end

  test "self references remain source references when no other named query matches", context do
    alert = Repo.update!(Ecto.Changeset.change(context.alert, name: "logs"))
    reject_full_catalogs()

    assert execute(alert) =~ String.replace(to_string(context.source.token), "-", "_")
  end

  test "the calling alert is excluded when another reference loads the catalogs", context do
    alert = Repo.update!(Ecto.Changeset.change(context.alert, name: "logs"))
    insert(:endpoint, user: context.user, name: "other", query: "SELECT 1 AS n")
    query = "SELECT 1 AS n FROM logs JOIN other ON true"

    assert execute(%{alert | query: query}) =~
             String.replace(to_string(context.source.token), "-", "_")
  end

  test "empty and malformed queries fail without reading catalogs", context do
    reject_full_catalogs()
    reject(Endpoints, :list_query_names, 1)
    reject(BigQueryAdaptor, :execute_query, 3)

    for query <- ["", "SELECT FROM"] do
      assert {:error, _} = Alerting.execute_alert_query(%{context.alert | query: query})
    end
  end

  test "an unavailable name index propagates its error without loading catalogs", context do
    reject_full_catalogs()
    reject(BigQueryAdaptor, :execute_query, 3)

    stub(Endpoints, :list_query_names, fn _ ->
      raise DBConnection.ConnectionError, message: "name index unavailable"
    end)

    assert {:error, :database_unavailable} = Alerting.execute_alert_query(context.alert)
  end

  test "an unavailable referenced catalog propagates its error", context do
    insert(:endpoint, user: context.user, name: "logs")
    reject(BigQueryAdaptor, :execute_query, 3)

    stub(Alerting, :list_alert_queries_by_user_id, fn _ ->
      raise DBConnection.ConnectionError, message: "alert catalog unavailable"
    end)

    assert {:error, :database_unavailable} = Alerting.execute_alert_query(context.alert)
  end

  defp reject_full_catalogs do
    reject(Endpoints, :list_endpoints_by, 1)
    reject(Alerting, :list_alert_queries_by_user_id, 1)
  end

  defp execute(alert) do
    assert {:ok, %{rows: [%{"n" => 1}]}} = Alerting.execute_alert_query(alert)
    assert_receive {:executed, sql}
    sql
  end
end
