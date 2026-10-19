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
      endpoint = insert_versioned_endpoint(user, backend)

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
        insert_versioned_endpoint(
          user,
          backend,
          [proactive_requerying_seconds: 1],
          %{"query" => "SELECT concat('historical-', toString(generateUUIDv4())) AS testing"}
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
      endpoint = insert_versioned_endpoint(user, backend)

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
               Endpoints.update_query(
                 user,
                 endpoint,
                 %{query: "select 'updated' as testing"},
                 user
               )

      TestUtils.retry_assert(fn ->
        refute Process.alive?(latest_cache_pid)
      end)

      assert Process.alive?(versioned_cache_pid)
      Endpoints.ResultsCache.invalidate(versioned_cache_pid)
    end

    @tag :clickhouse_cache
    test "refreshed versioned caches use current sandboxable setting", %{
      user: user,
      clickhouse_backend: backend
    } do
      test_pid = self()

      for {params, historical_sandboxable, expected} <- [
            {%{"sql" => "select 'override' as testing"}, true, "override"},
            {%{"lql" => "testing:historical s:testing"}, false, "historical"}
          ] do
        endpoint =
          insert_versioned_endpoint(
            user,
            backend,
            [
              sandboxable: true,
              query: "WITH data AS (SELECT 'current' AS testing) SELECT testing FROM data"
            ],
            %{
              "query" =>
                "WITH data AS (SELECT 'historical' AS testing UNION ALL SELECT 'discard' AS testing) SELECT testing FROM data",
              "sandboxable" => historical_sandboxable
            }
          )

        assert {:ok, snapshot} = Endpoints.get_endpoint_query_at_version(endpoint, 1)
        assert snapshot.sandboxable == historical_sandboxable
        query = %{snapshot | sandboxable: endpoint.sandboxable}
        endpoint_id = endpoint.id

        stub(ClickHouseAdaptor, :execute_query, fn backend, sql, opts ->
          send(test_pid, {:backend_query, endpoint_id})
          Mimic.call_original(ClickHouseAdaptor, :execute_query, [backend, sql, opts])
        end)

        assert {:ok, %{rows: [%{"testing" => ^expected}]}} =
                 Endpoints.run_cached_query(query, params)

        assert_received {:backend_query, ^endpoint_id}

        cache_pid = query |> Endpoints.ResultsCache.name(params) |> GenServer.whereis()
        monitor = Process.monitor(cache_pid)

        assert {:ok, endpoint} =
                 Endpoints.update_query(user, endpoint, %{sandboxable: false}, user)

        Logflare.ContextCache.bust_keys([{Endpoints, endpoint.id}])
        assert Process.alive?(cache_pid)

        send(cache_pid, :refresh)
        assert_receive {:DOWN, ^monitor, :process, ^cache_pid, :normal}, 5_000
        refute_received {:backend_query, ^endpoint_id}
      end
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

      started_at = System.monotonic_time(:millisecond)
      {:ok, cache_pid} = start_supervised({Logflare.Endpoints.ResultsCache, {endpoint, %{}, []}})
      monitor_ref = Process.monitor(cache_pid)
      assert Process.alive?(cache_pid)

      # First query should succeed
      assert {:ok, %{rows: [%{"testing" => "123"}]}} = Endpoints.run_cached_query(endpoint)

      assert_receive {:DOWN, ^monitor_ref, :process, ^cache_pid, :normal},
                     endpoint.cache_duration_seconds * 1_000 + 500

      elapsed = System.monotonic_time(:millisecond) - started_at
      assert elapsed >= endpoint.cache_duration_seconds * 1_000
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

      started_at = System.monotonic_time(:millisecond)
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
        send(test_pid, {refresh_ref, System.monotonic_time(:millisecond)})
        {:ok, TestUtils.gen_bq_response(test_response)}
      end)

      assert {:ok, %{rows: [%{"testing" => "123"}]}} = Endpoints.run_cached_query(endpoint)

      assert_receive {^refresh_ref, refreshed_at},
                     endpoint.proactive_requerying_seconds * 1_000 + 500

      assert refreshed_at - started_at >= endpoint.proactive_requerying_seconds * 1_000

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

  defp insert_versioned_endpoint(user, backend, attrs \\ [], snapshot_overrides \\ %{}) do
    endpoint =
      insert(
        :endpoint,
        Keyword.merge(
          [
            user: user,
            backend: backend,
            language: :ch_sql,
            query: "SELECT 'current' AS testing",
            cache_duration_seconds: 60,
            proactive_requerying_seconds: 60
          ],
          attrs
        )
      )

    insert(:endpoint_version,
      endpoint: endpoint,
      version_number: 1,
      snapshot_overrides:
        Map.merge(%{"query" => "SELECT 'historical' AS testing"}, snapshot_overrides)
    )

    endpoint
  end
end
