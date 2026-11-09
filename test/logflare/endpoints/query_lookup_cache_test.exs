defmodule Logflare.Endpoints.QueryLookupCacheTest do
  use Logflare.DataCase

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Backends.Adaptor.QueryResult
  alias Logflare.Endpoints
  alias Logflare.Sql
  alias Logflare.Users

  setup do
    insert(:plan)
    user = insert(:user)
    backend = insert(:backend, user: user, type: :clickhouse)

    endpoint =
      insert(:endpoint,
        user: user,
        backend: backend,
        language: :ch_sql,
        query: "SELECT 1 AS n"
      )

    %{user: user, backend: backend, endpoint: endpoint}
  end

  for explicit_backend? <- [true, false] do
    @tag explicit_backend?: explicit_backend?
    test "warm uncached executions reuse lookup caches (explicit backend: #{explicit_backend?})",
         context do
      endpoint =
        if context.explicit_backend?,
          do: context.endpoint,
          else: %{context.endpoint | backend_id: nil}

      expect(ClickHouseAdaptor, :execute_query, 2, fn backend, _, _ ->
        assert backend.id == context.backend.id
        {:ok, QueryResult.new([%{"n" => 1}], %{total_bytes_processed: 0})}
      end)

      assert {:ok, %{rows: [%{"n" => 1}]}} = Endpoints.run_query(endpoint)

      reject(Backends, :get_backend, 1)
      reject(Backends, :list_backends, 1)
      reject(Backends, :list_backends_by_user_id, 1)

      ref = :telemetry_test.attach_event_handlers(self(), [[:logflare, :repo, :query]])
      on_exit(fn -> :telemetry.detach(ref) end)

      assert {:ok, %{rows: [%{"n" => 1}]}} = Endpoints.run_query(endpoint)
      refute_received {[:logflare, :repo, :query], ^ref, _, _}
    end
  end

  test "a missing language-specific backend still falls back to the user's default", context do
    endpoint = %{context.endpoint | backend_id: nil, language: :bq_sql}

    expect(BigQueryAdaptor, :execute_query, fn backend, _, _ ->
      assert backend.type == :bigquery
      assert backend.id == nil
      assert backend.user_id == context.user.id
      {:ok, QueryResult.new([%{"n" => 1}], %{total_bytes_processed: 0})}
    end)

    assert {:ok, %{rows: [%{"n" => 1}]}} = Endpoints.run_query(endpoint)
  end

  test "SQL transformation accepts a cached user id without reading the user again", context do
    assert Users.Cache.get(context.user.id).id == context.user.id
    reject(Users, :get, 1)
    assert {:ok, sql} = Sql.transform(:ch_sql, "SELECT 1 AS n", context.user.id)
    assert sql == elem(Sql.transform(:ch_sql, "SELECT 1 AS n", context.user), 1)
  end

  test "SQL transformation preserves missing users and cache unavailability", context do
    expect(Users.Cache, :get, fn _ -> nil end)
    assert {:error, "User not found"} = Sql.transform(:ch_sql, "SELECT 1", context.user.id)

    expect(Users.Cache, :get, fn _ -> {:error, :database_unavailable} end)

    assert {:error, :database_unavailable} =
             Sql.transform(:ch_sql, "SELECT 1", context.user.id)
  end

  test "explicit backend misses and cache failures remain controlled errors", context do
    expect(Backends.Cache, :get_backend, fn _ -> nil end)
    assert {:error, "Backend not found"} = Endpoints.run_query(context.endpoint)

    expect(Backends.Cache, :get_backend, fn _ -> {:error, :database_unavailable} end)
    assert {:error, :database_unavailable} = Endpoints.run_query(context.endpoint)
  end

  test "backend list cache failures propagate rather than falling back", context do
    expect(Backends.Cache, :list_backends, fn _ -> {:error, :database_unavailable} end)

    assert {:error, :database_unavailable} =
             Endpoints.run_query(%{context.endpoint | backend_id: nil})
  end
end
