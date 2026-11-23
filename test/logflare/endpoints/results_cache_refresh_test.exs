defmodule Logflare.Endpoints.ResultsCacheRefreshTest do
  use Logflare.DataCase

  alias Logflare.Backends.Adaptor.QueryResult
  alias Logflare.Endpoints.ResultsCache

  setup do
    insert(:plan)
    user = insert(:user)
    backend = insert(:backend, user: user, type: :clickhouse)
    %{user: user, backend: backend}
  end

  for interval <- [0, -1, nil] do
    @tag interval: interval
    @tag real_expiry?: interval == 0
    test "interval #{inspect(interval)} executes once per cache lifetime without background refresh",
         context do
      endpoint =
        build(:endpoint,
          user: context.user,
          backend: context.backend,
          language: :ch_sql,
          query: "SELECT 1 AS n",
          cache_duration_seconds: if(context.real_expiry?, do: 1, else: 60)
        )
        |> change(proactive_requerying_seconds: context.interval)
        |> Repo.insert!()

      expect(ClickHouseAdaptor, :execute_query, 2, fn _, _, _ ->
        {:ok, QueryResult.new([%{"n" => 1}], %{total_bytes_processed: 0})}
      end)

      cache = start_supervised!({ResultsCache, {endpoint, %{}, []}})
      monitor = Process.monitor(cache)
      assert :sys.get_state(cache).refresh_timer == nil

      for _ <- 1..3 do
        assert {:ok, %{rows: [%{"n" => 1}]}} = ResultsCache.query(cache)
      end

      assert :sys.get_state(cache).refresh_timer == nil

      unless context.real_expiry?, do: send(cache, :shutdown)
      assert_receive {:DOWN, ^monitor, :process, ^cache, :normal}, 2_000

      replacement = start_supervised!({ResultsCache, {endpoint, %{}, []}})
      assert {:ok, %{rows: [%{"n" => 1}]}} = ResultsCache.query(replacement)
      assert :sys.get_state(replacement).refresh_timer == nil
    end
  end

  test "a surviving cache stops after loading an endpoint with a null TTL", context do
    endpoint =
      insert(:endpoint,
        user: context.user,
        backend: context.backend,
        language: :ch_sql,
        query: "SELECT 1 AS n",
        cache_duration_seconds: 60,
        proactive_requerying_seconds: 60
      )

    cache = start_supervised!({ResultsCache, {endpoint, %{}, []}})
    monitor = Process.monitor(cache)

    endpoint
    |> change(cache_duration_seconds: nil)
    |> Repo.update!()

    expect(ClickHouseAdaptor, :execute_query, fn _, _, _ ->
      {:ok, QueryResult.new([%{"n" => 1}], %{total_bytes_processed: 0})}
    end)

    assert {:ok, %{rows: [%{"n" => 1}]}} = ResultsCache.query(cache)
    assert_receive {:DOWN, ^monitor, :process, ^cache, :normal}, 1_000
  end

  test "positive intervals refresh and reschedule while retaining the TTL", context do
    endpoint =
      insert(:endpoint,
        user: context.user,
        backend: context.backend,
        language: :ch_sql,
        query: "SELECT 1 AS n",
        cache_duration_seconds: 60,
        proactive_requerying_seconds: 1
      )

    test_pid = self()

    expect(ClickHouseAdaptor, :execute_query, 2, fn _, _, _ ->
      send(test_pid, :executed)
      {:ok, QueryResult.new([%{"n" => 1}], %{total_bytes_processed: 0})}
    end)

    cache = start_supervised!({ResultsCache, {endpoint, %{}, []}})
    initial_state = :sys.get_state(cache)
    assert is_reference(initial_state.refresh_timer)
    assert {:ok, %{rows: [%{"n" => 1}]}} = ResultsCache.query(cache)
    assert_receive :executed

    after_query = :sys.get_state(cache)
    assert is_reference(after_query.refresh_timer)
    assert after_query.refresh_timer != initial_state.refresh_timer
    assert Process.read_timer(initial_state.refresh_timer) == false

    assert_receive :executed, 2_000

    TestUtils.retry_assert(fn ->
      state = :sys.get_state(cache)
      assert state.query_tasks == []
      assert is_reference(state.refresh_timer)
      assert state.refresh_timer != after_query.refresh_timer
      assert state.shutdown_timer == initial_state.shutdown_timer
    end)
  end
end
