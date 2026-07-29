defmodule Logflare.Endpoints.CacheTest do
  use Logflare.DataCase

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.QueryError
  alias Logflare.Endpoints

  describe "cache behavior" do
    setup do
      user = insert(:user)

      endpoint =
        insert(:endpoint,
          user: user,
          query: "select current_datetime() as testing",
          proactive_requerying_seconds: 1,
          cache_duration_seconds: 3
        )

      endpoint_2 =
        insert(:endpoint,
          user: user,
          query: "select current_datetime() as testing",
          proactive_requerying_seconds: 3,
          cache_duration_seconds: 1
        )

      _plan = insert(:plan, name: "Free")

      %{user: user, endpoint: endpoint, endpoint_2: endpoint_2}
    end

    setup context do
      if context[:clickhouse_cache] do
        {_source, backend} = setup_clickhouse_test(user: context.user)
        start_supervised!({ClickHouseAdaptor, backend})

        %{clickhouse_backend: backend}
      else
        :ok
      end
    end

    test "cache starts and serves cached results", %{endpoint: endpoint} do
      # Mock response by setting up test backend
      test_response = [%{"testing" => "123"}]

      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response(test_response)}
      end)

      # Start cache process
      {:ok, cache_pid} = start_supervised({Logflare.Endpoints.ResultsCache, {endpoint, %{}, []}})
      assert Process.alive?(cache_pid)

      # First query should hit backend
      assert {:ok, %{rows: [%{"testing" => "123"}]}} = Endpoints.run_cached_query(endpoint)

      # Second query should hit cache without calling backend again
      assert {:ok, %{rows: [%{"testing" => "123"}]}} = Endpoints.run_cached_query(endpoint)
    end

    @tag :clickhouse_cache
    test "cache separates current and versioned endpoint results", %{
      user: user,
      clickhouse_backend: backend
    } do
      endpoint =
        insert(:endpoint,
          user: user,
          backend: backend,
          language: :ch_sql,
          query: "SELECT 'current' AS testing",
          cache_duration_seconds: 60,
          proactive_requerying_seconds: 60
        )

      insert(:endpoint_version,
        endpoint: endpoint,
        version_number: 1,
        snapshot_overrides: %{"query" => "SELECT 'historical' AS testing"}
      )

      assert {:ok, versioned_endpoint} = Endpoints.get_endpoint_query_at_version(endpoint, 1)

      assert {:ok, %{rows: [%{"testing" => "current"}]}} = Endpoints.run_cached_query(endpoint)

      assert {:ok, %{rows: [%{"testing" => "historical"}]}} =
               Endpoints.run_cached_query(versioned_endpoint)

      assert {:ok, %{rows: [%{"testing" => "current"}]}} = Endpoints.run_cached_query(endpoint)

      assert {:ok, %{rows: [%{"testing" => "historical"}]}} =
               Endpoints.run_cached_query(versioned_endpoint)
    end

    @tag :clickhouse_cache
    test "versioned cache refresh keeps running the selected snapshot", %{
      user: user,
      clickhouse_backend: backend
    } do
      endpoint =
        insert(:endpoint,
          user: user,
          backend: backend,
          language: :ch_sql,
          query: "SELECT 'current' AS testing",
          cache_duration_seconds: 60,
          proactive_requerying_seconds: 1
        )

      insert(:endpoint_version,
        endpoint: endpoint,
        version_number: 1,
        snapshot_overrides: %{
          "query" => "SELECT concat('historical-', toString(generateUUIDv4())) AS testing",
          "cache_duration_seconds" => 60,
          "proactive_requerying_seconds" => 1
        }
      )

      assert {:ok, versioned_endpoint} = Endpoints.get_endpoint_query_at_version(endpoint, 1)

      assert {:ok, %{rows: [%{"testing" => first_value}]}} =
               Endpoints.run_cached_query(versioned_endpoint)

      assert String.starts_with?(first_value, "historical-")

      endpoint_id = endpoint.id

      Logflare.Repo.update_all(
        from(endpoint_query in Endpoints.EndpointQuery, where: endpoint_query.id == ^endpoint_id),
        set: [query: "SELECT 'current updated' AS testing"]
      )

      Process.sleep(versioned_endpoint.proactive_requerying_seconds * 1000 + 100)

      TestUtils.retry_assert(fn ->
        assert {:ok, %{rows: [%{"testing" => second_value}]}} =
                 Endpoints.run_cached_query(versioned_endpoint)

        assert String.starts_with?(second_value, "historical-")
        assert second_value != first_value
      end)

      versioned_endpoint
      |> Endpoints.ResultsCache.name(%{})
      |> GenServer.whereis()
      |> Endpoints.ResultsCache.invalidate()
    end

    @tag :clickhouse_cache
    test "endpoint updates invalidate latest caches without touching versioned caches", %{
      user: user,
      clickhouse_backend: backend
    } do
      endpoint =
        insert(:endpoint,
          user: user,
          backend: backend,
          language: :ch_sql,
          query: "SELECT 'current' AS testing",
          cache_duration_seconds: 60,
          proactive_requerying_seconds: 60
        )

      insert(:endpoint_version,
        endpoint: endpoint,
        version_number: 1,
        snapshot_overrides: %{
          "query" => "SELECT 'historical' AS testing",
          "cache_duration_seconds" => 60,
          "proactive_requerying_seconds" => 60
        }
      )

      assert {:ok, versioned_endpoint} = Endpoints.get_endpoint_query_at_version(endpoint, 1)

      assert {:ok, %{rows: [%{"testing" => "current"}]}} = Endpoints.run_cached_query(endpoint)

      assert {:ok, %{rows: [%{"testing" => "historical"}]}} =
               Endpoints.run_cached_query(versioned_endpoint)

      latest_cache_pid =
        endpoint
        |> Endpoints.ResultsCache.name(%{})
        |> GenServer.whereis()

      versioned_cache_pid =
        versioned_endpoint
        |> Endpoints.ResultsCache.name(%{})
        |> GenServer.whereis()

      assert is_pid(latest_cache_pid)
      assert is_pid(versioned_cache_pid)

      assert {:ok, _updated_endpoint} =
               Endpoints.update_query(endpoint, %{query: "select 'updated' as testing"}, user)

      TestUtils.retry_assert(fn ->
        refute Process.alive?(latest_cache_pid)
      end)

      assert Process.alive?(versioned_cache_pid)
      Endpoints.ResultsCache.invalidate(versioned_cache_pid)
    end

    test "cache dies on timeout error from query", %{endpoint: endpoint} do
      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:error, :timeout}
      end)

      {:ok, cache_pid} = start_supervised({Logflare.Endpoints.ResultsCache, {endpoint, %{}, []}})
      assert Process.alive?(cache_pid)

      assert {:error,
              %QueryError{
                kind: :connection_error,
                backend: Logflare.Backends.Adaptor.BigQueryAdaptor,
                raw_error: :timeout
              }} = Endpoints.run_cached_query(endpoint)

      refute Process.alive?(cache_pid)
    end

    test "cache dies on timeout from query task", %{endpoint: endpoint} do
      test_response = [%{"testing" => "123"}]

      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response(test_response)}
      end)

      {:ok, cache_pid} = start_supervised({Logflare.Endpoints.ResultsCache, {endpoint, %{}, []}})
      assert Process.alive?(cache_pid)

      # First query should succeed
      assert {:ok, %{rows: [%{"testing" => "123"}]}} = Endpoints.run_cached_query(endpoint)

      # Mock error response for refresh task
      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:error, :timeout}
      end)

      # should be larger than :proactive_requerying_seconds
      Process.sleep(endpoint.proactive_requerying_seconds * 1000 + 100)

      refute Process.alive?(cache_pid)
    end

    test "cache handles BigQuery error response bodies", %{endpoint: endpoint} do
      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:error, TestUtils.gen_bq_error("BQ Error")}
      end)

      {:ok, cache_pid} = start_supervised({Logflare.Endpoints.ResultsCache, {endpoint, %{}, []}})
      assert Process.alive?(cache_pid)

      assert {:error,
              %QueryError{
                kind: :backend_error,
                backend: Logflare.Backends.Adaptor.BigQueryAdaptor,
                raw_error: %{"message" => "BQ Error"}
              }} = Endpoints.run_cached_query(endpoint)

      refute Process.alive?(cache_pid)
    end

    test "cache dies after cache_duration_seconds", %{endpoint: endpoint} do
      test_response = [%{"testing" => "123"}]

      expected_calls =
        (endpoint.cache_duration_seconds / endpoint.proactive_requerying_seconds) |> floor()

      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, expected_calls, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response(test_response)}
      end)

      {:ok, cache_pid} = start_supervised({Logflare.Endpoints.ResultsCache, {endpoint, %{}, []}})
      assert Process.alive?(cache_pid)

      # First query should succeed
      assert {:ok, %{rows: [%{"testing" => "123"}]}} = Endpoints.run_cached_query(endpoint)

      # Cache should still be alive before cache_duration_seconds
      Process.sleep(endpoint.cache_duration_seconds * 1000 - 100)
      assert Process.alive?(cache_pid)

      # Cache should die after cache_duration_seconds
      Process.sleep(endpoint.cache_duration_seconds * 1000 + 100)
      refute Process.alive?(cache_pid)
    end

    test "cache dies after cache_duration_seconds gets set to 0", %{endpoint: endpoint} do
      test_response = [%{"testing" => "123"}]

      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 2, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response(test_response)}
      end)

      {:ok, cache_pid} = start_supervised({Logflare.Endpoints.ResultsCache, {endpoint, %{}, []}})
      assert Process.alive?(cache_pid)

      # First query should succeed
      assert {:ok, %{rows: [%{"testing" => "123"}]}} = Endpoints.run_cached_query(endpoint)
      assert Process.alive?(cache_pid)

      assert Logflare.Repo.update_all(Endpoints.EndpointQuery, set: [cache_duration_seconds: 0]) ==
               {2, nil}

      assert Logflare.ContextCache.bust_keys([{Logflare.Endpoints, endpoint.id}]) == {:ok, 1}

      # Cache should still be alive before cache_duration_seconds
      Process.sleep(endpoint.proactive_requerying_seconds * 1000 * 2)
      refute Process.alive?(cache_pid)
    end

    test "cache updates cached results after proactive_requerying_seconds", %{endpoint: endpoint} do
      test_response = [%{"testing" => "123"}]

      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response(test_response)}
      end)

      {:ok, cache_pid} = start_supervised({Logflare.Endpoints.ResultsCache, {endpoint, %{}, []}})
      assert Process.alive?(cache_pid)

      # First query should return first test response
      assert {:ok, %{rows: [%{"testing" => "123"}]}} = Endpoints.run_cached_query(endpoint)

      # Cache should still return first response before proactive_requerying_seconds
      Process.sleep(endpoint.proactive_requerying_seconds * 500)
      assert {:ok, %{rows: [%{"testing" => "123"}]}} = Endpoints.run_cached_query(endpoint)

      test_response = [%{"testing" => "456"}]

      GoogleApi.BigQuery.V2.Api.Jobs
      |> stub(:bigquery_jobs_query, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response(test_response)}
      end)

      # After proactive_requerying_seconds, should return updated response
      Process.sleep(endpoint.proactive_requerying_seconds * 1000 + 100)
      assert {:ok, %{rows: [%{"testing" => "456"}]}} = Endpoints.run_cached_query(endpoint)

      TestUtils.retry_assert(fn ->
        refute Process.alive?(cache_pid)
      end)
    end

    test "cache dies after cache_duration_seconds after proactive requery ", %{user: user} do
      endpoint =
        insert(:endpoint,
          user: user,
          query: "select current_datetime() as testing",
          proactive_requerying_seconds: 1,
          cache_duration_seconds: 3
        )

      # perform at most 2 queries before dying
      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 3, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response()}
      end)

      {:ok, cache_pid} = start_supervised({Logflare.Endpoints.ResultsCache, {endpoint, %{}, []}})
      assert Process.alive?(cache_pid)
      assert {:ok, %{rows: [_]}} = Endpoints.run_cached_query(endpoint)

      Process.sleep(700)
      assert {:ok, %{rows: [_]}} = Endpoints.run_cached_query(endpoint)

      # should terminate after cache_duration_seconds
      Process.sleep(2500)
      refute Process.alive?(cache_pid)
    end

    test "endpoint 2: cache dies before proactive query", %{endpoint_2: endpoint} do
      test_response = [%{"testing" => "123"}]

      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response(test_response)}
      end)

      {:ok, cache_pid} = start_supervised({Logflare.Endpoints.ResultsCache, {endpoint, %{}, []}})
      assert Process.alive?(cache_pid)

      # First query should succeed
      assert {:ok, %{rows: [%{"testing" => "123"}]}} = Endpoints.run_cached_query(endpoint)

      reject(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 4)

      # should be larger than :proactive_requerying_seconds
      Process.sleep(endpoint.proactive_requerying_seconds * 1000 + 100)

      refute Process.alive?(cache_pid)
    end
  end
end
