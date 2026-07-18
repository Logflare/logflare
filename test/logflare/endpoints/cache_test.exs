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
          cache_duration_seconds: 2
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
      endpoint = %{endpoint | proactive_requerying_seconds: 3}
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

      monitor_ref = Process.monitor(cache_pid)
      send(cache_pid, :refresh)
      assert_receive {:DOWN, ^monitor_ref, :process, ^cache_pid, :normal}, 1_500
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

      monitor_ref = Process.monitor(cache_pid)

      refute_receive {:DOWN, ^monitor_ref, :process, ^cache_pid, :normal},
                     div(endpoint.cache_duration_seconds * 1000, 4)

      assert Process.alive?(cache_pid)

      # Cache should die after cache_duration_seconds
      assert_receive {:DOWN, ^monitor_ref, :process, ^cache_pid, :normal},
                     endpoint.cache_duration_seconds * 1000
    end

    test "cache dies after cache_duration_seconds gets set to 0", %{endpoint: endpoint} do
      endpoint = %{endpoint | proactive_requerying_seconds: 3}
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

      monitor_ref = Process.monitor(cache_pid)
      send(cache_pid, :refresh)
      assert_receive {:DOWN, ^monitor_ref, :process, ^cache_pid, :normal}, 1_500
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
      assert {:ok, %{rows: [%{"testing" => "123"}]}} = Endpoints.run_cached_query(endpoint)

      test_response = [%{"testing" => "456"}]
      test_pid = self()
      refresh_ref = make_ref()

      GoogleApi.BigQuery.V2.Api.Jobs
      |> stub(:bigquery_jobs_query, fn _conn, _proj_id, _opts ->
        send(test_pid, refresh_ref)
        {:ok, TestUtils.gen_bq_response(test_response)}
      end)

      assert_receive ^refresh_ref, endpoint.proactive_requerying_seconds * 1000 + 500

      TestUtils.retry_assert(fn ->
        assert {:ok, %{rows: [%{"testing" => "456"}]}} = Endpoints.run_cached_query(endpoint)
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

      # The initial query and two proactive refreshes must run before expiry.
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
      monitor_ref = Process.monitor(cache_pid)

      assert_receive {:DOWN, ^monitor_ref, :process, ^cache_pid, :normal},
                     endpoint.cache_duration_seconds * 1000
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

      monitor_ref = Process.monitor(cache_pid)

      assert_receive {:DOWN, ^monitor_ref, :process, ^cache_pid, :normal},
                     endpoint.cache_duration_seconds * 1000 + 500
    end
  end
end
