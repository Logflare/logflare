defmodule Logflare.Endpoints.CacheTest do
  use Logflare.DataCase

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

    test "cache separates current and versioned endpoint results", %{endpoint_2: endpoint} do
      insert(:endpoint_version,
        endpoint: endpoint,
        version_number: 1,
        snapshot_overrides: %{"query" => "select 'historical' as testing"}
      )

      assert {:ok, versioned_endpoint} = Endpoints.get_endpoint_query_at_version(endpoint, 1)

      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 2, fn _conn, _proj_id, opts ->
        cond do
          opts[:body].query =~ "historical" ->
            {:ok, TestUtils.gen_bq_response([%{"testing" => "historical"}])}

          opts[:body].query =~ "current_datetime" ->
            {:ok, TestUtils.gen_bq_response([%{"testing" => "current"}])}
        end
      end)

      assert {:ok, %{rows: [%{"testing" => "current"}]}} = Endpoints.run_cached_query(endpoint)

      assert {:ok, %{rows: [%{"testing" => "historical"}]}} =
               Endpoints.run_cached_query(versioned_endpoint)

      assert {:ok, %{rows: [%{"testing" => "current"}]}} = Endpoints.run_cached_query(endpoint)

      assert {:ok, %{rows: [%{"testing" => "historical"}]}} =
               Endpoints.run_cached_query(versioned_endpoint)
    end

    test "versioned cache refresh keeps running the selected snapshot", %{endpoint: endpoint} do
      insert(:endpoint_version,
        endpoint: endpoint,
        version_number: 1,
        snapshot_overrides: %{
          "query" => "select 'historical' as testing",
          "cache_duration_seconds" => 3,
          "proactive_requerying_seconds" => 1
        }
      )

      assert {:ok, versioned_endpoint} = Endpoints.get_endpoint_query_at_version(endpoint, 1)

      {:ok, responses} = Agent.start_link(fn -> ["first", "second"] end)
      pid = self()

      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 2, fn _conn, _proj_id, opts ->
        send(pid, {:query, opts[:body].query})

        value =
          Agent.get_and_update(responses, fn
            [value | rest] -> {value, rest}
            [] -> {"extra", []}
          end)

        {:ok, TestUtils.gen_bq_response([%{"testing" => value}])}
      end)

      assert {:ok, %{rows: [%{"testing" => "first"}]}} =
               Endpoints.run_cached_query(versioned_endpoint)

      Logflare.Repo.update_all(Endpoints.EndpointQuery,
        set: [query: "select 'current updated' as testing"]
      )

      Process.sleep(versioned_endpoint.proactive_requerying_seconds * 1000 + 100)

      assert {:ok, %{rows: [%{"testing" => "second"}]}} =
               Endpoints.run_cached_query(versioned_endpoint)

      assert_received {:query, first_query}
      assert_received {:query, second_query}
      assert first_query =~ "historical"
      assert second_query =~ "historical"

      versioned_endpoint
      |> Endpoints.ResultsCache.name(%{})
      |> GenServer.whereis()
      |> Endpoints.ResultsCache.invalidate()
    end

    test "endpoint updates invalidate versioned caches", %{endpoint: endpoint, user: user} do
      insert(:endpoint_version,
        endpoint: endpoint,
        version_number: 1,
        snapshot_overrides: %{"query" => "select 'historical' as testing"}
      )

      assert {:ok, versioned_endpoint} = Endpoints.get_endpoint_query_at_version(endpoint, 1)

      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([%{"testing" => "historical"}])}
      end)

      {:ok, cache_pid} =
        start_supervised({Logflare.Endpoints.ResultsCache, {versioned_endpoint, %{}, []}})

      assert {:ok, %{rows: [%{"testing" => "historical"}]}} =
               Endpoints.run_cached_query(versioned_endpoint)

      assert {:ok, _updated_endpoint} =
               Endpoints.update_query(endpoint, %{query: "select 'updated' as testing"}, user)

      TestUtils.retry_assert(fn ->
        refute Process.alive?(cache_pid)
      end)
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
