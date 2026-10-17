defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.QueryClassExecutionTest do
  use Logflare.DataCase

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.QueryError
  alias Logflare.Endpoints

  @policy %{
    "default" => %{"priority" => 5},
    "api_free" => %{"priority" => 10, "max_threads" => 4},
    "api_paid" => %{"priority" => 10, "max_threads" => 4},
    "mcp" => %{"priority" => 10, "max_threads" => 4},
    "dashboard_logs_free" => %{"priority" => 1},
    "dashboard_logs_paid" => %{"priority" => 1},
    "dashboard_reports_free" => %{"priority" => 1},
    "dashboard_reports_paid" => %{"priority" => 1},
    "dashboard_observability" => %{"priority" => 1}
  }

  setup do
    insert(:plan)
    admin = insert(:user, admin: true)

    {_source, original} =
      setup_clickhouse_test(
        config: %{
          read_only_urls: %{"dashboard_logs_paid" => "http://localhost:8123"},
          default_read_cluster: "dashboard_logs_paid"
        }
      )

    assert {:ok, backend} = Backends.configure_query_class_settings(admin, original, @policy)
    start_supervised!({ClickHouseAdaptor, backend})
    %{backend: backend, original: original, admin: admin}
  end

  test "effective priorities use requested classes despite routing fallback", %{backend: backend} do
    for {label, priority} <-
          Enum.map(@policy, fn {label, settings} -> {label, settings["priority"]} end) ++
            [{nil, 5}, {"unknown", 5}] do
      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:ok, {[%{"priority" => ^priority}], _}} =
                   ClickHouseAdaptor.execute_ch_query(
                     backend,
                     "SELECT toInt64(getSetting('priority')) AS priority",
                     [],
                     read_cluster: label
                   )
        end)

      if label in ["api_free", "api_paid", "mcp"] do
        refute log =~ "read cluster not configured"
      end
    end
  end

  test "connection fallback retains the requested API policy", context do
    assert {:ok, backend} =
             Backends.update_backend(context.backend, %{
               config: %{
                 read_only_urls: %{
                   "api_paid" => "http://localhost:8123",
                   "dashboard_logs_paid" => "http://localhost:8123"
                 }
               }
             })

    api_pool = ClickHouseAdaptor.connection_pool_via(backend, "api_paid")

    stub(Ch, :query, fn
      ^api_pool, _sql, _params, _opts ->
        {:error, %DBConnection.ConnectionError{message: "unreachable"}}

      pool, sql, params, opts ->
        Mimic.call_original(Ch, :query, [pool, sql, params, opts])
    end)

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        assert {:ok, {[%{"priority" => 10}], _}} =
                 ClickHouseAdaptor.execute_ch_query(
                   backend,
                   "SELECT toInt64(getSetting('priority')) AS priority",
                   [],
                   read_cluster: "api_paid"
                 )
      end)

    assert log =~ "read cluster unhealthy"
  end

  test "results stay class-agnostic while refreshes use live policy and the original class",
       context do
    endpoint =
      insert(:endpoint,
        user: Logflare.Users.get(context.backend.user_id),
        backend: context.backend,
        language: :ch_sql,
        query: "SELECT 'cached-policy' AS value",
        cache_duration_seconds: 60,
        proactive_requerying_seconds: 60
      )

    test_pid = self()

    stub(Ch, :query, fn conn, sql, params, opts ->
      sql = IO.iodata_to_binary(sql)
      if sql =~ "cached-policy", do: send(test_pid, {:executed, sql})
      Mimic.call_original(Ch, :query, [conn, sql, params, opts])
    end)

    pid = start_supervised!({Endpoints.ResultsCache, {endpoint, %{}, [read_cluster: "api_paid"]}})

    assert {:ok, %{rows: [%{"value" => "cached-policy"}]}} =
             Endpoints.run_cached_query(endpoint, %{}, read_cluster: "api_paid")

    assert_received {:executed, first_sql}
    assert first_sql =~ "priority = 10"

    assert {:ok, %{rows: [%{"value" => "cached-policy"}]}} =
             Endpoints.run_cached_query(endpoint, %{}, read_cluster: "dashboard_logs_paid")

    refute_received {:executed, _}

    policy = Map.put(@policy, "api_paid", %{"priority" => 12, "max_threads" => 2})

    pool_via = ClickHouseAdaptor.connection_pool_via(context.backend, "dashboard_logs_paid")
    pool_pid = GenServer.whereis(pool_via)
    assert is_pid(pool_pid)
    assert Backends.Cache.get_backend(context.backend.id).config.query_class_settings == @policy

    assert {:ok, _} =
             Backends.configure_query_class_settings(context.admin, context.backend, policy)

    assert Backends.Cache.get_backend(context.backend.id).config.query_class_settings == policy
    assert GenServer.whereis(pool_via) == pool_pid
    send(pid, :refresh)
    assert_receive {:executed, refreshed_sql}, 5_000
    assert refreshed_sql =~ "priority = 12"
    assert refreshed_sql =~ "max_threads = 2"
    TestUtils.retry_assert(fn -> assert :sys.get_state(pid).query_tasks == [] end)
  end

  test "endpoint policy merges once, handles converted parameters and matches previews",
       context do
    backend = context.backend

    endpoint =
      insert(:endpoint,
        user: Logflare.Users.get(backend.user_id),
        backend: backend,
        language: :ch_sql,
        query: "SELECT @value AS value",
        enforced_clickhouse_settings: %{"max_threads" => 2, "max_execution_time" => 0.5}
      )

    assert {:ok, preview} =
             Endpoints.get_transformed_query(endpoint, %{"value" => "hello"},
               read_cluster: "api_paid"
             )

    assert preview =~ "priority = 10"
    assert preview =~ "max_threads = 2"
    assert preview =~ "max_execution_time = 0.5"

    assert {:ok, %{rows: [%{"value" => "hello"}]}} =
             Endpoints.run_query(endpoint, %{"value" => "hello"},
               read_cluster: "api_paid",
               enforced_clickhouse_settings: %{"priority" => 1}
             )

    assert {:error, %QueryError{kind: :invalid_query, description: reason}} =
             ClickHouseAdaptor.execute_ch_query(
               backend,
               "SELECT 1 AS n SETTINGS priority = 0",
               [],
               read_cluster: "api_paid"
             )

    assert reason =~ "priority is enforced"
  end

  test "effective API settings are recorded in system.query_log.Settings", context do
    id = Ecto.UUID.generate()

    assert {:ok, _} =
             ClickHouseAdaptor.execute_ch_query(
               context.backend,
               "SELECT 1 AS n SETTINGS log_queries = 1, log_query_settings = 1",
               [],
               read_cluster: "api_paid",
               headers: [{"x-clickhouse-query-id", id}]
             )

    assert {:ok, _} = ClickHouseAdaptor.execute_ch_query(context.original, "SYSTEM FLUSH LOGS")

    TestUtils.retry_assert(fn ->
      assert {:ok, {[%{"priority" => "10", "threads" => "4"}], _}} =
               ClickHouseAdaptor.execute_ch_query(
                 context.original,
                 "SELECT Settings['priority'] AS priority, Settings['max_threads'] AS threads " <>
                   "FROM system.query_log WHERE query_id = '#{id}' AND type = 'QueryFinish'"
               )
    end)
  end

  test "all allowed keys work under readonly=2 and profile ceilings remain enforced", context do
    name = "query_class_reader_#{Ecto.UUID.generate() |> String.replace("-", "")}"
    admin_backend = context.original

    assert {:ok, _} =
             ClickHouseAdaptor.execute_ch_query(
               admin_backend,
               "CREATE USER #{name} IDENTIFIED BY 'test-secret' SETTINGS " <>
                 "readonly = 2 READONLY, priority = 5 MIN 1, max_threads = 8 MAX 8, " <>
                 "max_execution_time = 2 MAX 2, max_memory_usage = 10000000 MAX 10000000, " <>
                 "max_bytes_to_read = 1000000 MAX 1000000, max_rows_to_read = 1000 MAX 1000"
             )

    uri = URI.parse(admin_backend.config.url)

    admin_opts =
      [
        scheme: uri.scheme,
        hostname: uri.host,
        port: uri.port,
        database: admin_backend.config.database,
        username: Map.get(admin_backend.config, :username),
        password: Map.get(admin_backend.config, :password)
      ]
      |> Enum.reject(fn {_key, value} -> is_nil(value) end)

    on_exit(fn ->
      {:ok, pool} = Ch.start_link(admin_opts)

      try do
        assert {:ok, _} = Ch.query(pool, "DROP USER IF EXISTS #{name}")
      after
        GenServer.stop(pool)
      end
    end)

    assert {:ok, _} =
             ClickHouseAdaptor.execute_ch_query(admin_backend, "GRANT SELECT ON *.* TO #{name}")

    policy = %{
      "default" => %{"priority" => 5},
      "api_paid" => %{
        "priority" => 10,
        "max_threads" => 4,
        "max_execution_time" => 1.5,
        "max_memory_usage" => 1_000_000,
        "max_bytes_to_read" => 100_000,
        "max_rows_to_read" => 100
      }
    }

    assert {:ok, backend} =
             Backends.configure_query_class_settings(context.admin, context.backend, policy)

    assert {:ok, backend} =
             Backends.update_backend(backend, %{
               config: %{query_user: name, query_password: "test-secret"}
             })

    keys =
      ~w(priority max_threads max_execution_time max_memory_usage max_bytes_to_read max_rows_to_read read_overflow_mode timeout_overflow_mode readonly)

    sql = "SELECT " <> Enum.map_join(keys, ", ", &"toString(getSetting('#{&1}')) AS #{&1}")

    TestUtils.retry_assert(fn ->
      assert {:ok, {[settings], _}} =
               ClickHouseAdaptor.execute_ch_query(backend, sql, [], read_cluster: "api_paid")

      assert settings == %{
               "priority" => "10",
               "max_threads" => "4",
               "max_execution_time" => "1.5",
               "max_memory_usage" => "1000000",
               "max_bytes_to_read" => "100000",
               "max_rows_to_read" => "100",
               "read_overflow_mode" => "throw",
               "timeout_overflow_mode" => "throw",
               "readonly" => "2"
             }
    end)

    assert {:error, %QueryError{}} =
             ClickHouseAdaptor.execute_ch_query(backend, "SELECT 1", [],
               enforced_clickhouse_settings: %{"max_memory_usage" => 20_000_000}
             )

    assert {:ok, backend} =
             Backends.update_backend(backend, %{config: %{query_user: nil, query_password: nil}})

    TestUtils.retry_assert(fn ->
      assert {:ok, {[%{"readonly" => 0}], _}} =
               ClickHouseAdaptor.execute_ch_query(
                 backend,
                 "SELECT toInt64(getSetting('readonly')) AS readonly"
               )
    end)
  end
end
