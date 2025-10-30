defmodule Logflare.Backends.Adaptor.ClickhouseAdaptorTest do
  use Logflare.DataCase, async: false

  import Ecto.Query
  import Logflare.Utils.Guards

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.ClickhouseAdaptor
  alias Logflare.Backends.Adaptor.ClickhouseAdaptor.ConnectionManager
  alias Logflare.Backends.Adaptor.ClickhouseAdaptor.QueryConnectionSup
  alias Logflare.Backends.Ecto.SqlUtils
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
               "ingest_logs_#{stringified_source_token}"
    end

    test "`clickhouse_ingest_table_name/1` will raise an exception if the table name is equal to or exceeds 200 chars",
         %{source: source} do
      assert_raise RuntimeError,
                   ~r/^The dynamically generated ClickHouse resource name starting with `ingest_logs_/,
                   fn ->
                     source
                     |> modify_source_with_long_token()
                     |> ClickhouseAdaptor.clickhouse_ingest_table_name()
                   end
    end
  end

  describe "connection and basic functionality" do
    setup do
      insert(:plan, name: "Free")

      {source, backend, cleanup_fn} = setup_clickhouse_test()
      on_exit(cleanup_fn)

      start_supervised!({ClickhouseAdaptor, {source, backend}})

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

  describe "redact_config/1" do
    test "redacts password field" do
      config = %{password: "secret123", database: "logs"}
      assert %{password: "REDACTED"} = ClickhouseAdaptor.redact_config(config)
    end
  end

  describe "log event insertion and retrieval" do
    setup do
      insert(:plan, name: "Free")

      {source, backend, cleanup_fn} = setup_clickhouse_test()
      on_exit(cleanup_fn)

      start_supervised!({ClickhouseAdaptor, {source, backend}})
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
          "SELECT payload FROM #{table_name} ORDER BY timestamp"
        )

      assert {:ok, rows} = query_result
      assert length(rows) == 2

      row_payloads = Enum.map(rows, &Jason.decode!(&1["payload"]))

      assert [%{"event_message" => "Test message 1"}, %{"event_message" => "Test message 2"}] =
               row_payloads
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

      start_supervised!({ClickhouseAdaptor, {source, backend}})

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

    test "converts Ecto queries to ClickHouse SQL format" do
      query =
        from(l in "logs",
          where: fragment("? ~ ?", l.event_message, ^"error.*timeout") and l.level == ^"error",
          select: l.event_message
        )

      {:ok, {pg_sql, pg_params}} = SqlUtils.ecto_to_pg_sql(query)

      converted_param = SqlUtils.normalize_datetime_param("error.*timeout")

      assert converted_param == "error.*timeout"

      converted_sql = SqlUtils.pg_params_to_question_marks("SELECT * FROM logs WHERE level = $1")

      assert converted_sql == "SELECT * FROM logs WHERE level = ?"

      assert pg_sql =~ "SELECT "
      assert is_list(pg_params)
      assert "error.*timeout" in pg_params
      assert "error" in pg_params
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
      start_supervised!({ClickhouseAdaptor, {source1_with_backend, backend}})

      [backend: backend, user: user, source1: source1_with_backend, source2: source2]
    end

    test "multiple default ingest sources with same backend do not crash", %{
      backend: backend,
      source2: source2
    } do
      {:ok, _} = ClickhouseAdaptor.execute_ch_read_query(backend, "SELECT 1")

      initial_manager_via = Backends.via_backend(backend, ConnectionManager)
      initial_manager_pid = GenServer.whereis(initial_manager_via)

      assert is_pid(initial_manager_pid)
      assert Process.alive?(initial_manager_pid)
      assert ConnectionManager.pool_active?(backend)

      # start a second clickhouse adaptor with the same backend, but a different source
      {:ok, source2} = Backends.update_source_backends(source2, [backend])
      ch_pid2 = start_supervised!({ClickhouseAdaptor, {source2, backend}}, id: :adaptor2)

      Process.sleep(200)

      manager_via2 = Backends.via_backend(backend, ConnectionManager)
      manager_pid2 = GenServer.whereis(manager_via2)

      assert Process.alive?(ch_pid2)
      assert is_pid(manager_pid2)
      assert Process.alive?(manager_pid2)
      assert ConnectionManager.pool_active?(backend)

      assert initial_manager_pid == manager_pid2,
             "Both sources should share the same ConnectionManager"
    end
  end

  describe "ecto_to_sql/2" do
    test "converts Ecto query to ClickHouse SQL format" do
      query =
        from("test_table")
        |> select([t], %{id: t.id, value: t.value})
        |> where([t], t.id > ^1)

      {:ok, {sql, params}} = ClickhouseAdaptor.ecto_to_sql(query, [])

      assert is_non_empty_binary(sql)
      assert is_list(params)

      # Should contain basic query structure
      assert sql =~ "SELECT"
      assert sql =~ "FROM \"test_table\""
      assert sql =~ "WHERE"
      assert sql =~ "t0.\"id\" >"
      assert sql =~ "$0"

      # Parameters should be normalized
      assert length(params) == 1
      assert params == [1]
    end

    test "converts complex query with ClickHouse-specific transformations" do
      query =
        from("test_table")
        |> select([t], %{id: t.id, name: t.name})
        |> where([t], fragment("? ~ ?", t.name, ^"pattern"))

      {:ok, {sql, params}} = ClickhouseAdaptor.ecto_to_sql(query, [])

      assert is_binary(sql)
      assert is_list(params)

      # Should have the regex operator (transformation may not apply to fragment)
      assert sql =~ "~"

      assert length(params) == 1
      assert params == ["pattern"]
    end

    test "converts datetime parameters correctly in ClickHouse SQL format" do
      datetime = ~U[2023-12-25 10:30:45Z]

      query =
        from("test_table")
        |> select([t], %{id: t.id, timestamp: t.timestamp})
        |> where([t], t.timestamp > ^datetime)

      {:ok, {sql, params}} = ClickhouseAdaptor.ecto_to_sql(query, [])

      assert is_non_empty_binary(sql)
      assert is_list(params)

      # Should contain basic query structure
      assert sql =~ "SELECT"
      assert sql =~ "FROM \"test_table\""
      assert sql =~ "WHERE"
      assert sql =~ "t0.\"timestamp\" >"
      assert sql =~ "$0"

      # Parameters should be normalized
      assert ["2023-12-25 10:30:45Z"] = params
    end

    test "handles query conversion errors gracefully" do
      # Create an invalid query that should fail conversion
      invalid_query = %Ecto.Query{from: nil}

      assert {:error, _reason} = ClickhouseAdaptor.ecto_to_sql(invalid_query, [])
    end
  end

  describe "read query `ConnectionManager` automatic wake-up" do
    setup do
      insert(:plan, name: "Free")
      {source, backend, cleanup_fn} = setup_clickhouse_test()
      on_exit(cleanup_fn)

      start_supervised!({ClickhouseAdaptor, {source, backend}})
      Process.sleep(100)
      [source: source, backend: backend]
    end

    test "multiple concurrent read queries handle race conditions correctly", %{
      backend: backend
    } do
      via = Backends.via_backend(backend, ConnectionManager)
      refute GenServer.whereis(via)

      results =
        for i <- 1..10 do
          Task.async(fn ->
            ClickhouseAdaptor.execute_ch_read_query(backend, "SELECT #{i} as result")
          end)
        end
        |> Task.await_many(1_000)

      assert Enum.all?(results, &match?({:ok, [%{"result" => _}]}, &1))
      assert connection_manager_pid = GenServer.whereis(via)

      children =
        DynamicSupervisor.which_children(QueryConnectionSup.DynamicSupervisor)

      assert [_] = Enum.filter(children, &match?({_, ^connection_manager_pid, _, _}, &1))
    end

    test "`ConnectionManager` restarts if it crashes", %{backend: backend} do
      {:ok, _} = ClickhouseAdaptor.execute_ch_read_query(backend, "SELECT 1")

      via = Backends.via_backend(backend, ConnectionManager)
      assert original_pid = GenServer.whereis(via)

      Process.exit(original_pid, :kill)

      TestUtils.retry_assert(fn ->
        assert {:ok, [%{"result" => 2}]} =
                 ClickhouseAdaptor.execute_ch_read_query(backend, "SELECT 2 as result")

        assert new_pid = GenServer.whereis(via)
        assert new_pid != original_pid
        assert ConnectionManager.pool_active?(backend)
      end)
    end

    test "`ConnectionManager` for different backends are independent", %{
      source: source,
      backend: backend1
    } do
      assert QueryConnectionSup.count_query_connection_managers() == 0
      {_source2, backend2, cleanup_fn2} = setup_clickhouse_test(source: source)
      on_exit(cleanup_fn2)
      start_supervised!({ClickhouseAdaptor, {source, backend2}}, id: :adaptor2)

      {:ok, _} = ClickhouseAdaptor.execute_ch_read_query(backend2, "SELECT 1")
      assert ConnectionManager.pool_active?(backend2)

      via1 = Backends.via_backend(backend1, ConnectionManager)
      via2 = Backends.via_backend(backend2, ConnectionManager)

      refute GenServer.whereis(via2) == GenServer.whereis(via1)
      refute ConnectionManager.pool_active?(backend1)

      assert QueryConnectionSup.count_query_connection_managers() == 1
    end

    test "ingest and read pools are truly independent", %{source: source, backend: backend} do
      ingest_via = Backends.via_source(source, ConnectionManager, backend.id)
      assert ingest_cm_pid = GenServer.whereis(ingest_via)

      {:ok, _} = ClickhouseAdaptor.execute_ch_read_query(backend, "SELECT 1")
      query_via = Backends.via_backend(backend, ConnectionManager)
      assert query_cm_pid = GenServer.whereis(query_via)

      refute ingest_cm_pid == query_cm_pid,
             "Ingest and query ConnectionManagers should be separate processes"

      Process.exit(ingest_cm_pid, :kill)

      TestUtils.retry_assert(fn ->
        assert Process.alive?(query_cm_pid)
      end)

      assert {:ok, [%{"result" => 2}]} =
               ClickhouseAdaptor.execute_ch_read_query(backend, "SELECT 2 as result")
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
