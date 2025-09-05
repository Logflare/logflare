defmodule Logflare.Backends.Adaptor.ClickhouseAdaptorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.ClickhouseAdaptor
  alias Logflare.Backends.Adaptor.ClickhouseAdaptor.ConnectionManager
  alias Logflare.Sources.Source

  doctest ClickhouseAdaptor

  describe "table name generation" do
    setup do
      insert(:plan, name: "Free")

      {source, backend, cleanup_fn} = setup_clickhouse_test()
      on_exit(cleanup_fn)

      stringified_source_token =
        source.token
        |> Atom.to_string()
        |> String.replace("-", "_")

      [source: source, backend: backend, stringified_source_token: stringified_source_token]
    end

    test "`clickhouse_ingest_table_name/1` generates a unique log ingest table name based on the source token",
         %{source: source, stringified_source_token: stringified_source_token} do
      assert ClickhouseAdaptor.clickhouse_ingest_table_name(source) ==
               "log_events_#{stringified_source_token}"
    end

    test "`clickhouse_ingest_table_name/1` will raise an exception if the table name is equal to or exceeds 200 chars",
         %{source: source} do
      assert_raise RuntimeError,
                   ~r/^The dynamically generated ClickHouse resource name starting with `log_events_/,
                   fn ->
                     source
                     |> modify_source_with_long_token()
                     |> ClickhouseAdaptor.clickhouse_ingest_table_name()
                   end
    end

    test "`clickhouse_key_count_table_name/1` generates a unique key count table name based on the source token",
         %{source: source, stringified_source_token: stringified_source_token} do
      assert ClickhouseAdaptor.clickhouse_key_count_table_name(source) ==
               "key_type_counts_per_min_#{stringified_source_token}"
    end

    test "`clickhouse_key_count_table_name/1` will raise an exception if the table name is equal to or exceeds 200 chars",
         %{source: source} do
      assert_raise RuntimeError,
                   ~r/^The dynamically generated ClickHouse resource name starting with `key_type_counts_per_min_/,
                   fn ->
                     source
                     |> modify_source_with_long_token()
                     |> ClickhouseAdaptor.clickhouse_key_count_table_name()
                   end
    end

    test "`clickhouse_materialized_view_name/1` generates a unique mat view name based on the source token",
         %{source: source, stringified_source_token: stringified_source_token} do
      assert ClickhouseAdaptor.clickhouse_materialized_view_name(source) ==
               "mv_key_type_counts_per_min_#{stringified_source_token}"
    end

    test "`clickhouse_materialized_view_name/1` will raise an exception if the view name is equal to or exceeds 200 chars",
         %{source: source} do
      assert_raise RuntimeError,
                   ~r/^The dynamically generated ClickHouse resource name starting with `mv_key_type_counts_per_min_/,
                   fn ->
                     source
                     |> modify_source_with_long_token()
                     |> ClickhouseAdaptor.clickhouse_materialized_view_name()
                   end
    end
  end

  describe "connection and basic functionality" do
    setup do
      insert(:plan, name: "Free")

      {source, backend, cleanup_fn} = setup_clickhouse_test()
      on_exit(cleanup_fn)

      {:ok, _pid} = ClickhouseAdaptor.start_link({source, backend})

      [source: source, backend: backend]
    end

    test "can test connection using a `{source, backend}` tuple", %{
      source: source,
      backend: backend
    } do
      result = ClickhouseAdaptor.test_connection({source, backend})
      assert :ok = result
    end

    test "can test connection using a `backend` struct", %{
      backend: backend
    } do
      result = ClickhouseAdaptor.test_connection(backend)
      assert :ok = result
    end

    test "can execute ingest queries", %{source: source, backend: backend} do
      result =
        ClickhouseAdaptor.execute_ch_ingest_query({source, backend}, "SELECT 1 as test")

      assert {:ok, %Ch.Result{rows: [[1]]}} = result
    end

    test "can execute read queries", %{backend: backend} do
      result =
        ClickhouseAdaptor.execute_ch_read_query(backend, "SELECT 2 as test")

      assert {:ok, [%{"test" => 2}]} = result
    end

    test "handles query errors", %{backend: backend} do
      result =
        ClickhouseAdaptor.execute_ch_read_query(backend, "INVALID SQL QUERY")

      assert {:error, _} = result
    end
  end

  describe "log event insertion and retrieval" do
    setup do
      insert(:plan, name: "Free")

      {source, backend, cleanup_fn} = setup_clickhouse_test()
      on_exit(cleanup_fn)

      {:ok, _pid} = ClickhouseAdaptor.start_link({source, backend})
      assert {:ok, _} = ClickhouseAdaptor.provision_ingest_table({source, backend})

      [source: source, backend: backend]
    end

    test "can insert and retrieve log events", %{source: source, backend: backend} do
      log_events = [
        build(:log_event,
          source: source,
          message: "Test message 1",
          body: %{"level" => "info", "user_id" => 123}
        ),
        build(:log_event,
          source: source,
          message: "Test message 2",
          body: %{"level" => "error", "user_id" => 456}
        )
      ]

      result = ClickhouseAdaptor.insert_log_events({source, backend}, log_events)
      assert {:ok, %Ch.Result{}} = result

      Process.sleep(100)

      table_name = ClickhouseAdaptor.clickhouse_ingest_table_name(source)

      query_result =
        ClickhouseAdaptor.execute_ch_read_query(
          backend,
          "SELECT event_message, body FROM #{table_name} ORDER BY timestamp"
        )

      assert {:ok, rows} = query_result
      assert length(rows) == 2

      assert [%{"event_message" => "Test message 1"}, %{"event_message" => "Test message 2"}] =
               rows
    end

    test "handles empty event list", %{source: source, backend: backend} do
      result = ClickhouseAdaptor.insert_log_events({source, backend}, [])
      assert {:ok, %Ch.Result{}} = result
    end
  end

  describe "execute_query/2" do
    setup do
      insert(:plan, name: "Free")

      {source, backend, cleanup_fn} = setup_clickhouse_test()
      on_exit(cleanup_fn)

      {:ok, _pid} = ClickhouseAdaptor.start_link({source, backend})

      [source: source, backend: backend]
    end

    test "executes simple queries using backend-only interface", %{backend: backend} do
      result = ClickhouseAdaptor.execute_query(backend, "SELECT 1 as test_value", [])

      assert {:ok, [%{"test_value" => 1}]} = result
    end

    test "converts `@param` syntax to ClickHouse `{param:String}` format", %{backend: backend} do
      result =
        ClickhouseAdaptor.execute_query(
          backend,
          {"SELECT @test_value as param_result", ["test_value"], %{"test_value" => "hello"}},
          []
        )

      assert {:ok, [%{"param_result" => "hello"}]} = result
    end

    test "handles query errors gracefully", %{backend: backend} do
      result = ClickhouseAdaptor.execute_query(backend, "INVALID SQL SYNTAX", [])

      assert {:error, _error_message} = result
    end
  end

  describe "connection pool collision handling" do
    setup do
      insert(:plan, name: "Free")
      user = insert(:user)
      source1 = insert(:source, user: user, default_ingest_backend_enabled?: true)
      source2 = insert(:source, user: user, default_ingest_backend_enabled?: true)

      {source1_with_backend, backend, cleanup_fn} =
        setup_clickhouse_test(
          source: source1,
          user: user,
          default_ingest?: true
        )

      on_exit(cleanup_fn)

      {:ok, _pid} = ClickhouseAdaptor.start_link({source1_with_backend, backend})

      [backend: backend, user: user, source1: source1_with_backend, source2: source2]
    end

    test "multiple default ingest sources with same backend do not crash", %{
      backend: backend,
      source2: source2
    } do
      assert :ok = ConnectionManager.ensure_pool_started(backend)

      initial_manager_via = Backends.via_backend(backend, ConnectionManager)
      initial_manager_pid = GenServer.whereis(initial_manager_via)

      assert is_pid(initial_manager_pid)
      assert Process.alive?(initial_manager_pid)
      assert ConnectionManager.pool_active?(backend)

      # start a second clickhouse adaptor with the same backend, but a different source
      {:ok, source2} = Backends.update_source_backends(source2, [backend])
      {:ok, ch_pid2} = ClickhouseAdaptor.start_link({source2, backend})

      Process.sleep(200)

      manager_via2 = Backends.via_backend(backend, ConnectionManager)
      manager_pid2 = GenServer.whereis(manager_via2)

      assert Process.alive?(ch_pid2)
      assert is_pid(manager_pid2)
      assert Process.alive?(manager_pid2)
      assert ConnectionManager.pool_active?(backend)
    end
  end

  defp modify_source_with_long_token(%Source{} = source) do
    long_token = random_string(200) |> String.to_atom()

    %Source{
      source
      | token: long_token
    }
  end

  defp random_string(length) do
    alphanumeric = Enum.concat([?0..?9, ?a..?z])

    1..length
    |> Enum.map(fn _ -> Enum.random(alphanumeric) end)
    |> List.to_string()
  end
end
