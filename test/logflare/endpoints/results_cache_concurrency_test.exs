defmodule Logflare.Endpoints.ResultsCacheConcurrencyTest do
  use Logflare.DataCase

  alias Logflare.Backends.Adaptor.QueryResult
  alias Logflare.Endpoints
  alias Logflare.Endpoints.ResultsCache

  setup do
    insert(:plan)
    owner = insert(:user)
    admin = insert(:user, admin: true)
    backend = insert(:backend, user: owner, type: :clickhouse)

    endpoint =
      insert(:endpoint,
        user: owner,
        backend: backend,
        language: :ch_sql,
        query: "SELECT 1 AS n",
        cache_duration_seconds: 60,
        proactive_requerying_seconds: 60
      )

    cache = start_supervised!({ResultsCache, {endpoint, %{}, []}})
    %{endpoint: endpoint, owner: owner, admin: admin, cache: cache}
  end

  test "cold callers and refreshes share one execution", %{cache: cache} do
    block_query()
    callers = start_callers(cache)
    assert_receive {:query_started, pid}, 5_000
    send(cache, :refresh)
    assert length(:sys.get_state(cache).query_tasks) == 1
    refute_received {:query_started, _}
    send(pid, :finish)
    for caller <- callers, do: assert({:ok, %{rows: [%{"n" => 1}]}} = Task.await(caller))
    assert {:ok, %{rows: [%{"n" => 1}]}} = ResultsCache.query(cache)
    assert :sys.get_state(cache).query_tasks == []
  end

  test "operator saves drain foreground work while new callers use a replacement", context do
    block_query()
    callers = start_callers(context.cache)
    assert_receive {:query_started, pid}, 5_000
    cache_monitor = Process.monitor(context.cache)

    assert {:ok, current} =
             Endpoints.configure_enforced_clickhouse_settings(
               context.admin,
               context.endpoint,
               %{"max_execution_time" => 1}
             )

    assert current.enforced_clickhouse_settings["max_execution_time"] == 1
    assert Process.alive?(pid)
    assert {:error, :stale_cache} = ResultsCache.query(context.cache)
    assert {:ok, :draining} = ResultsCache.invalidate(context.cache)
    assert Endpoints.Resolver.list_caches(context.endpoint, :all_versions) == []

    expect(ClickHouseAdaptor, :execute_query, fn backend, {sql, _, _, query}, opts ->
      assert opts[:enforced_clickhouse_settings] == query.enforced_clickhouse_settings
      assert {:ok, sql} = ClickHouseAdaptor.prepare_query(backend, sql, opts)
      assert sql =~ "max_execution_time = 1"
      {:ok, QueryResult.new([%{"n" => 2}], %{total_bytes_processed: 0})}
    end)

    stub(Endpoints.Resolver, :resolve, fn query, params, opts ->
      Mimic.call_original(Endpoints.Resolver, :resolve, [query, params, opts])
    end)

    expect(Endpoints.Resolver, :resolve, fn _, _, _ -> context.cache end)

    assert {:ok, %{rows: [%{"n" => 2}]}} = Endpoints.run_cached_query(context.endpoint)
    replacement = GenServer.whereis(ResultsCache.name(context.endpoint, %{}))
    assert is_pid(replacement)
    assert replacement != context.cache
    on_exit(fn -> ResultsCache.invalidate(replacement) end)

    send(context.cache, :refresh)
    assert :sys.get_state(context.cache).refresh_timer == nil
    assert length(:sys.get_state(context.cache).query_tasks) == 1
    send(pid, :finish)

    for caller <- callers,
        do: assert({:ok, %{rows: [%{"n" => 1}]}} = Task.await(caller))

    assert_receive {:DOWN, ^cache_monitor, :process, _, :normal}
    assert GenServer.whereis(ResultsCache.name(context.endpoint, %{})) == replacement
    assert {:ok, %{rows: [%{"n" => 2}]}} = ResultsCache.query(replacement)
  end

  test "stale PID retries are bounded", context do
    assert {:ok, :stopped} = ResultsCache.invalidate(context.cache)
    expect(Endpoints.Resolver, :resolve, 3, fn _, _, _ -> context.cache end)

    assert {:error, %{"status" => "CACHE_REPLACED"}} =
             Endpoints.run_cached_query(context.endpoint)
  end

  test "ordinary cache expiry lets a pending foreground response finish", %{cache: cache} do
    block_query()
    callers = start_callers(cache)
    assert_receive {:query_started, pid}, 5_000
    monitor = Process.monitor(cache)
    send(cache, :shutdown)
    assert :sys.get_state(cache).shutdown_timer == nil
    assert Process.alive?(pid)
    send(pid, :finish)
    for caller <- callers, do: assert({:ok, %{rows: [%{"n" => 1}]}} = Task.await(caller))
    assert_receive {:DOWN, ^monitor, :process, ^cache, :normal}
  end

  test "task failures give waiting callers a controlled error", %{cache: cache} do
    monitor = Process.monitor(cache)
    expect(ClickHouseAdaptor, :execute_query, fn _, _, _ -> raise "query task failed" end)
    assert {:error, %{"status" => "CACHE_STOPPED"}} = ResultsCache.query(cache)
    assert_receive {:DOWN, ^monitor, :process, ^cache, :normal}
  end

  test "post-commit invalidation failures are explicit and stopped caches are idempotent",
       context do
    monitor = Process.monitor(context.cache)
    :ok = :sys.suspend(context.cache)

    try do
      assert {:error, {:settings_saved, updated, :cache_invalidation_failed}} =
               Endpoints.configure_enforced_clickhouse_settings(
                 context.admin,
                 context.endpoint,
                 %{"max_execution_time" => 1}
               )

      assert updated.enforced_clickhouse_settings["max_execution_time"] == 1

      assert Endpoints.get_endpoint_query(updated.id).enforced_clickhouse_settings ==
               updated.enforced_clickhouse_settings
    after
      :sys.resume(context.cache)
    end

    assert_receive {:DOWN, ^monitor, :process, _, :normal}, 5_000
    assert {:ok, :stopped} = ResultsCache.invalidate(context.cache)
  end

  test "ordinary updates drain pending callers without cancelling their query", context do
    block_query()
    callers = start_callers(context.cache)
    assert_receive {:query_started, pid}, 5_000
    monitor = Process.monitor(context.cache)

    assert {:ok, updated} =
             Endpoints.update_query(
               context.owner,
               context.endpoint,
               %{labels: "drain"},
               context.owner
             )

    assert updated.labels == "drain"
    assert Process.alive?(pid)
    send(pid, :finish)
    for caller <- callers, do: assert({:ok, %{rows: [%{"n" => 1}]}} = Task.await(caller))
    assert_receive {:DOWN, ^monitor, :process, _, :normal}
  end

  for operation <- [:update, :restore] do
    @tag operation: operation
    test "#{operation} reports committed changes when invalidation fails", context do
      insert(:endpoint_version,
        endpoint: context.endpoint,
        version_number: 1,
        snapshot_overrides: %{"query" => "SELECT 2 AS n"}
      )

      insert(:endpoint_version, endpoint: context.endpoint, version_number: 2)
      monitor = Process.monitor(context.cache)
      :ok = :sys.suspend(context.cache)

      try do
        result =
          case context.operation do
            :update ->
              Endpoints.update_query(
                context.owner,
                context.endpoint,
                %{query: "SELECT 3 AS n"},
                context.owner
              )

            :restore ->
              Endpoints.restore_query_version(context.owner, context.endpoint, 1, context.owner)
          end

        assert {:error, {:saved, updated, :cache_invalidation_failed}} = result
        assert Endpoints.get_endpoint_query(updated.id).query == updated.query
        assert updated.query != context.endpoint.query
      after
        :sys.resume(context.cache)
      end

      assert_receive {:DOWN, ^monitor, :process, _, :normal}, 5_000
    end
  end

  defp block_query do
    test_pid = self()

    expect(ClickHouseAdaptor, :execute_query, fn _, _, _ ->
      send(test_pid, {:query_started, self()})

      receive do
        :finish -> {:ok, QueryResult.new([%{"n" => 1}], %{total_bytes_processed: 0})}
      end
    end)

    :ok
  end

  defp start_callers(cache) do
    callers = for _ <- 1..2, do: Task.async(fn -> ResultsCache.query(cache) end)
    TestUtils.retry_assert(fn -> assert length(:sys.get_state(cache).waiting_callers) == 2 end)
    callers
  end
end
