defmodule Logflare.ClickHouseFixtureCleanupTest do
  use Logflare.DataCase

  alias Logflare.Backends
  alias Logflare.Backends.ConsolidatedSup

  setup do
    insert(:plan)

    stub(Ch, :query, fn conn, statement, params, opts ->
      Mimic.call_original(Ch, :query, [conn, statement, params, opts])
    end)

    {source, backend} = setup_clickhouse_test(cleanup?: false)

    tables =
      Enum.map(
        [:log, :metric, :trace],
        &ClickHouseAdaptor.clickhouse_ingest_table_name(backend, &1)
      )

    with_connection(source, backend, fn conn ->
      for table <- tables do
        assert {:ok, _} = query(conn, "CREATE TABLE #{table} (n UInt8) ENGINE = Memory")
      end
    end)

    on_exit(fn ->
      ConsolidatedSup.stop_pipeline(backend.id)

      with_connection(source, backend, fn conn ->
        for table <- tables do
          assert {:ok, _} = query(conn, "DROP TABLE IF EXISTS #{table} SYNC")
        end
      end)
    end)

    %{source: source, backend: backend, tables: tables}
  end

  test "drops every type table and closes its cleanup connection", context do
    assert :ok = cleanup_clickhouse_tables(context.backend)
    assert_tables_removed(context)
    assert GenServer.whereis(Backends.via_backend(context.backend, Logflare.DataCase)) == nil
  end

  test "waits for Atomic table deletion instead of leaving a deferred-drop queue", context do
    with_connection(context.source, context.backend, fn conn ->
      table = hd(context.tables)
      assert {:ok, _} = query(conn, "DROP TABLE #{table} SYNC")

      assert {:ok, _} =
               query(conn, "CREATE TABLE #{table} (n UInt8) ENGINE = MergeTree ORDER BY n")

      assert {:ok, _} = query(conn, "INSERT INTO #{table} SETTINGS async_insert = 0 VALUES (1)")
    end)

    assert :ok = cleanup_clickhouse_tables(context.backend)

    with_connection(context.source, context.backend, fn conn ->
      assert {:ok, %Ch.Result{rows: [[0]]}} =
               Mimic.call_original(Ch, :query, [
                 conn,
                 "SELECT count() FROM system.dropped_tables " <>
                   "WHERE database = currentDatabase() AND table IN {tables:Array(String)}",
                 %{"tables" => context.tables},
                 []
               ])
    end)
  end

  test "cleanup is idempotent when tables and the ingest pipeline are already gone", context do
    assert :ok = cleanup_clickhouse_tables(context.backend)
    assert :ok = cleanup_clickhouse_tables(context.backend)
    assert_tables_removed(context)
  end

  test "stops a globally supervised ingest pipeline before removing its tables", context do
    assert {:ok, pid} = ConsolidatedSup.start_pipeline(context.backend)
    ref = Process.monitor(pid)

    assert :ok = cleanup_clickhouse_tables(context.backend)
    assert_receive {:DOWN, ^ref, :process, ^pid, _reason}
    refute ConsolidatedSup.pipeline_running?(context.backend)
    assert_tables_removed(context)
  end

  test "test mocks cannot intercept teardown or consume its queries", context do
    reject(ClickHouseAdaptor, :execute_ch_query, 4)
    reject(Ch, :start_link, 1)
    reject(Ch, :query, 4)

    assert :ok = cleanup_clickhouse_tables(context.backend)
    assert_tables_removed(context)
  end

  test "SQL cleanup errors fail visibly and still close the cleanup connection", context do
    invalid_backend = %{context.backend | token: "invalid token"}

    assert_raise MatchError, fn -> cleanup_clickhouse_tables(invalid_backend) end
    assert GenServer.whereis(Backends.via_backend(context.backend, Logflare.DataCase)) == nil
  end

  @spec assert_tables_removed(map()) :: :ok
  defp assert_tables_removed(context) do
    with_connection(context.source, context.backend, fn conn ->
      for table <- context.tables do
        assert {:ok, %Ch.Result{rows: [[0]]}} = query(conn, "EXISTS TABLE #{table}")
      end
    end)

    :ok
  end

  @spec with_connection(struct(), struct(), (pid() -> term())) :: term()
  defp with_connection(source, backend, fun) do
    opts =
      build_clickhouse_connection_opts(source, backend, :query)
      |> Keyword.merge(pool_size: 1, name: Backends.via_backend(backend, __MODULE__))

    {:ok, conn} = Mimic.call_original(Ch, :start_link, [opts])

    try do
      fun.(conn)
    after
      GenServer.stop(conn)
    end
  end

  @spec query(pid(), String.t()) :: {:ok, Ch.Result.t()} | {:error, term()}
  defp query(conn, statement), do: Mimic.call_original(Ch, :query, [conn, statement, [], []])
end
