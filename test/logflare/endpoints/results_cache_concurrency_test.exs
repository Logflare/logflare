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
    %{endpoint: endpoint, admin: admin, cache: cache}
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

  test "operator saves cancel foreground work and respond to every waiting caller", context do
    block_query()
    callers = start_callers(context.cache)
    assert_receive {:query_started, pid}, 5_000
    task_monitor = Process.monitor(pid)
    cache_monitor = Process.monitor(context.cache)

    assert {:ok, current} =
             Endpoints.configure_enforced_clickhouse_settings(
               context.admin,
               context.endpoint,
               %{"max_execution_time" => 1}
             )

    assert current.enforced_clickhouse_settings["max_execution_time"] == 1
    assert_receive {:DOWN, ^task_monitor, :process, ^pid, :killed}
    assert_receive {:DOWN, ^cache_monitor, :process, _, :normal}

    for caller <- callers,
        do: assert({:error, %{"status" => "CACHE_STOPPED"}} = Task.await(caller))
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
