defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.AsyncInsertFallbackTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.AsyncInsertFallback

  setup do
    insert(:plan, name: "Free")

    backend =
      insert(:backend, type: :clickhouse)
      |> Map.put(:config, %{
        async_insert_mode: "all_batches",
        async_insert_max_rows: 2,
        async_insert_cluster_url: "http://dedicated.local"
      })

    Application.put_env(:logflare, AsyncInsertFallback,
      min_attempts: 4,
      min_failures: 2,
      failure_ratio: 0.5,
      window_ms: 30_000,
      cooldown_ms: 60_000,
      probe_timeout_ms: 45_000
    )

    on_exit(fn -> Application.delete_env(:logflare, AsyncInsertFallback) end)

    pid = start_supervised!({AsyncInsertFallback, backend})
    [backend: backend, pid: pid]
  end

  test "uses split routing when the breaker is unavailable", %{backend: backend} do
    missing = %{backend | id: backend.id + 1_000_000}

    assert :split = AsyncInsertFallback.route(missing, 2)

    assert :ok =
             AsyncInsertFallback.record_result(missing, {:normal, make_ref()}, {:error, :timeout})
  end

  test "opens after enough eligible failures, then splits both large and small batches", %{
    backend: backend
  } do
    assert {:all, first} = AsyncInsertFallback.route(backend, 2)
    assert :ok = AsyncInsertFallback.record_result(backend, first, {:error, :timeout})
    assert {:all, second} = AsyncInsertFallback.route(backend, 2)
    assert :ok = AsyncInsertFallback.record_result(backend, second, :ok)
    assert {:all, third} = AsyncInsertFallback.route(backend, 2)

    assert :ok =
             AsyncInsertFallback.record_result(
               backend,
               third,
               {:error, {:http, 500, "ASYNC_INSERT failed"}}
             )

    assert {:all, fourth} = AsyncInsertFallback.route(backend, 2)
    assert :ok = AsyncInsertFallback.record_result(backend, fourth, :ok)

    assert AsyncInsertFallback.get_state(backend).phase == :open
    assert :split = AsyncInsertFallback.route(backend, 2)
    assert :split = AsyncInsertFallback.route(backend, 1)
    assert :ok = AsyncInsertFallback.record_result(backend, fourth, {:error, :timeout})
    assert AsyncInsertFallback.get_state(backend).phase == :open
  end

  test "ignores unrelated errors and too many parts rather than shifting load to sync", %{
    backend: backend
  } do
    for reason <- [
          :pool_timeout,
          {:http, 400, "bad schema"},
          {:http, 429, "throttled"},
          {:http, 500, "primary overloaded"},
          {:http, 500, "TOO_MANY_PARTS"}
        ] do
      assert {:all, token} = AsyncInsertFallback.route(backend, 2)
      assert :ok = AsyncInsertFallback.record_result(backend, token, {:error, reason})
    end

    assert AsyncInsertFallback.get_state(backend).phase == :closed
  end

  test "only one large batch probes after cooldown, and success closes the breaker", %{
    backend: backend,
    pid: pid
  } do
    open_breaker(backend)
    expire_cooldown(pid)

    assert :split = AsyncInsertFallback.route(backend, 1)

    routes =
      1..8
      |> Task.async_stream(fn _ -> AsyncInsertFallback.route(backend, 2) end, max_concurrency: 8)
      |> Enum.map(fn {:ok, route} -> route end)

    assert [{:all, {:probe, probe_ref}}] = Enum.filter(routes, &match?({:all, _}, &1))
    assert Enum.count(routes, &(&1 == :split)) == 7

    assert :ok = AsyncInsertFallback.record_result(backend, {:probe, probe_ref}, :ok)
    assert AsyncInsertFallback.get_state(backend).phase == :closed
    assert {:all, {:normal, _}} = AsyncInsertFallback.route(backend, 2)
  end

  test "failed or lost probes reopen, and a later probe can recover", %{
    backend: backend,
    pid: pid
  } do
    open_breaker(backend)
    expire_cooldown(pid)
    assert {:all, {:probe, failed_ref}} = AsyncInsertFallback.route(backend, 2)

    assert :ok =
             AsyncInsertFallback.record_result(backend, {:probe, failed_ref}, {:error, :timeout})

    assert :split = AsyncInsertFallback.route(backend, 2)

    expire_cooldown(pid)
    assert {:all, {:probe, lost_ref}} = AsyncInsertFallback.route(backend, 2)

    :sys.replace_state(
      pid,
      &%{&1 | probe_started_at: System.monotonic_time(:millisecond) - 46_000}
    )

    assert :split = AsyncInsertFallback.route(backend, 2)
    assert :ok = AsyncInsertFallback.record_result(backend, {:probe, lost_ref}, :ok)
    assert AsyncInsertFallback.get_state(backend).phase == :open

    expire_cooldown(pid)
    assert {:all, {:probe, recovered_ref}} = AsyncInsertFallback.route(backend, 2)
    assert :ok = AsyncInsertFallback.record_result(backend, {:probe, recovered_ref}, :ok)
    assert AsyncInsertFallback.get_state(backend).phase == :closed
  end

  test "a backend config revision clears the fallback and stale results", %{backend: backend} do
    assert {:all, old_token} = AsyncInsertFallback.route(backend, 2)
    open_breaker(backend)
    updated = %{backend | updated_at: NaiveDateTime.add(backend.updated_at, 1, :second)}

    assert {:all, {:normal, _}} = AsyncInsertFallback.route(updated, 2)
    assert :ok = AsyncInsertFallback.record_result(updated, old_token, {:error, :timeout})
    assert AsyncInsertFallback.get_state(updated).phase == :closed
  end

  test "the rolling window expires prior failures", %{backend: backend, pid: pid} do
    assert {:all, token} = AsyncInsertFallback.route(backend, 2)
    assert :ok = AsyncInsertFallback.record_result(backend, token, {:error, :timeout})

    :sys.replace_state(pid, fn state ->
      %{state | outcomes: [{System.monotonic_time(:millisecond) - 31_000, :failure}]}
    end)

    assert {:all, next_token} = AsyncInsertFallback.route(backend, 2)
    assert :ok = AsyncInsertFallback.record_result(backend, next_token, {:error, :timeout})
    assert length(AsyncInsertFallback.get_state(backend).outcomes) == 1
  end

  test "transition telemetry reports fallback and recovery", %{backend: backend, pid: pid} do
    TestUtils.attach_forwarder([:logflare, :clickhouse, :async_insert_fallback, :transition])
    open_breaker(backend)

    assert_receive {:telemetry_event,
                    [:logflare, :clickhouse, :async_insert_fallback, :transition], %{count: 1},
                    %{backend_id: backend_id, from: :closed, to: :open}}

    assert backend_id == backend.id
    expire_cooldown(pid)
    assert {:all, {:probe, ref}} = AsyncInsertFallback.route(backend, 2)
    assert :ok = AsyncInsertFallback.record_result(backend, {:probe, ref}, :ok)

    assert_receive {:telemetry_event,
                    [:logflare, :clickhouse, :async_insert_fallback, :transition], %{count: 1},
                    %{from: :open, to: :half_open}}

    assert_receive {:telemetry_event,
                    [:logflare, :clickhouse, :async_insert_fallback, :transition], %{count: 1},
                    %{from: :half_open, to: :closed}}
  end

  defp open_breaker(backend) do
    for outcome <- [{:error, :timeout}, :ok, {:error, :timeout}, :ok] do
      assert {:all, token} = AsyncInsertFallback.route(backend, 2)
      assert :ok = AsyncInsertFallback.record_result(backend, token, outcome)
    end

    assert AsyncInsertFallback.get_state(backend).phase == :open
  end

  defp expire_cooldown(pid) do
    :sys.replace_state(pid, &%{&1 | open_until: System.monotonic_time(:millisecond) - 1})
  end
end
