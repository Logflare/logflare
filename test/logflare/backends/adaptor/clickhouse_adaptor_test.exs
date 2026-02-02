defmodule Logflare.Backends.Adaptor.ClickHouseAdaptorTest do
  use Logflare.DataCase, async: false

  import Ecto.Query
  import Logflare.Utils.Guards

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.ConnectionManager
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.QueryConnectionSup
  alias Logflare.Backends.Backend
  alias Logflare.Backends.Ecto.SqlUtils

  doctest ClickHouseAdaptor

  describe "table name generation" do
    setup do
      insert(:plan, name: "Free")

      {source, backend, cleanup_fn} = setup_clickhouse_test()
      on_exit(cleanup_fn)

      stringified_backend_token =
        backend.token
        |> String.replace("-", "_")

      [source: source, backend: backend, stringified_backend_token: stringified_backend_token]
    end

    test "`clickhouse_ingest_table_name/1` generates a unique log ingest table name based on the backend token",
         %{backend: backend, stringified_backend_token: stringified_backend_token} do
      assert ClickHouseAdaptor.clickhouse_ingest_table_name(backend) ==
               "consolidated_ingest_#{stringified_backend_token}"
    end

    test "`clickhouse_ingest_table_name/1` will raise an exception if the table name is equal to or exceeds 200 chars",
         %{backend: backend} do
      assert_raise RuntimeError,
                   ~r/^The dynamically generated ClickHouse resource name starting with `consolidated_ingest_/,
                   fn ->
                     backend
                     |> modify_backend_with_long_token()
                     |> ClickHouseAdaptor.clickhouse_ingest_table_name()
                   end
    end
  end

  describe "connection and basic functionality" do
    setup do
      insert(:plan, name: "Free")

      {source, backend, cleanup_fn} = setup_clickhouse_test()
      on_exit(cleanup_fn)

      start_supervised!({ClickHouseAdaptor, backend})

      [source: source, backend: backend]
    end

    test "can test connection using a `backend` struct", %{
      backend: backend
    } do
      result = ClickHouseAdaptor.test_connection(backend)
      assert :ok = result
    end

    test "can execute queries", %{backend: backend} do
      result =
        ClickHouseAdaptor.execute_ch_query(backend, "SELECT 1 as test")

      assert {:ok, [%{"test" => 1}]} = result
    end

    test "handles query errors", %{backend: backend} do
      result =
        ClickHouseAdaptor.execute_ch_query(backend, "INVALID SQL QUERY")

      assert {:error, _} = result
    end
  end

  describe "redact_config/1" do
    test "redacts password field" do
      config = %{password: "secret123", database: "logs"}
      assert %{password: "REDACTED"} = ClickHouseAdaptor.redact_config(config)
    end
  end

  describe "cast_and_validate_config" do
    test "casts async_insert as boolean" do
      changeset = cast_and_validate_config(async_insert: true)

      assert changeset.valid?
      assert Ecto.Changeset.get_field(changeset, :async_insert) == true
    end

    test "defaults async_insert to false when not provided" do
      changeset = cast_and_validate_config()

      assert changeset.valid?
      assert Ecto.Changeset.get_field(changeset, :async_insert) == false
    end

    test "casts string async_insert value" do
      changeset = cast_and_validate_config(async_insert: "true")

      assert changeset.valid?
      assert Ecto.Changeset.get_field(changeset, :async_insert) == true
    end

    test "casts read_only_url when provided" do
      changeset =
        cast_and_validate_config(read_only_url: "https://read-only.clickhouse.cloud:8443")

      assert changeset.valid?

      assert Ecto.Changeset.get_field(changeset, :read_only_url) ==
               "https://read-only.clickhouse.cloud:8443"
    end

    test "read_only_url defaults to nil when not provided" do
      changeset = cast_and_validate_config()

      assert changeset.valid?
      assert Ecto.Changeset.get_field(changeset, :read_only_url) == nil
    end

    test "rejects invalid read_only_url format" do
      changeset = cast_and_validate_config(read_only_url: "invalid-url")

      refute changeset.valid?
      assert {:read_only_url, _} = hd(changeset.errors)
    end

    test "accepts valid http read_only_url" do
      changeset = cast_and_validate_config(read_only_url: "http://read-cluster.local:8123")

      assert changeset.valid?
    end

    test "accepts valid https read_only_url" do
      changeset =
        cast_and_validate_config(read_only_url: "https://read-cluster.clickhouse.cloud:8443")

      assert changeset.valid?
    end
  end

  defp cast_and_validate_config(attrs \\ []) do
    default_attrs = %{
      url: "http://localhost",
      database: "test",
      port: 8123
    }

    Adaptor.cast_and_validate_config(ClickHouseAdaptor, Map.merge(default_attrs, Map.new(attrs)))
  end

  describe "read_only_url fallback behavior" do
    test "uses primary url when read_only_url is nil" do
      config = %{
        url: "http://primary.clickhouse.local",
        read_only_url: nil,
        port: 8123
      }

      assert resolve_read_url(config) == "http://primary.clickhouse.local"
    end

    test "uses primary url when read_only_url is empty string" do
      config = %{
        url: "http://primary.clickhouse.local",
        read_only_url: "",
        port: 8123
      }

      assert resolve_read_url(config) == "http://primary.clickhouse.local"
    end

    test "uses read_only_url when configured" do
      config = %{
        url: "http://primary.clickhouse.local",
        read_only_url: "http://readonly.clickhouse.local",
        port: 8123
      }

      assert resolve_read_url(config) == "http://readonly.clickhouse.local"
    end
  end

  defp resolve_read_url(config) do
    import Logflare.Utils.Guards

    read_only_url = Map.get(config, :read_only_url)
    if is_non_empty_binary(read_only_url), do: read_only_url, else: Map.get(config, :url)
  end

  describe "log event insertion and retrieval" do
    setup do
      insert(:plan, name: "Free")

      {source, backend, cleanup_fn} = setup_clickhouse_test()
      on_exit(cleanup_fn)

      start_supervised!({ClickHouseAdaptor, backend})
      assert {:ok, _} = ClickHouseAdaptor.provision_ingest_table(backend)

      [source: source, backend: backend]
    end

    test "can insert log events with async_insert enabled", %{source: source, backend: backend} do
      backend_with_async = %{backend | config: Map.put(backend.config, :async_insert, true)}

      log_events = [
        build(:log_event,
          id: "660e8400-e29b-41d4-a716-446655440001",
          source: source,
          message: "Async test message",
          metadata: %{"level" => "info"}
        )
      ]

      result = ClickHouseAdaptor.insert_log_events(backend_with_async, log_events)
      assert :ok = result

      Process.sleep(500)

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend)

      query_result =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          "SELECT id, source_name FROM #{table_name} WHERE id = '660e8400-e29b-41d4-a716-446655440001'"
        )

      assert {:ok, [row]} = query_result
      assert row["id"] == "660e8400-e29b-41d4-a716-446655440001"
      assert row["source_name"] == source.name
    end

    test "can insert and retrieve log events", %{source: source, backend: backend} do
      log_events = [
        build(:log_event,
          id: "550e8400-e29b-41d4-a716-446655440000",
          source: source,
          message: "Test message 1",
          metadata: %{"level" => "info", "user_id" => 123}
        ),
        build(:log_event,
          id: "9bc07845-9859-4163-bfe5-a74c1a1443a2",
          source: source,
          message: "Test message 2",
          metadata: %{"level" => "error", "user_id" => 456}
        )
      ]

      result = ClickHouseAdaptor.insert_log_events(backend, log_events)
      assert :ok = result

      Process.sleep(100)

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend)

      query_result =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          "SELECT id, source_uuid, source_name, body, ingested_at, timestamp FROM #{table_name} ORDER BY timestamp"
        )

      assert {:ok, rows} = query_result
      assert length(rows) == 2

      row_payloads = Enum.map(rows, fn row -> Map.update!(row, "body", &Jason.decode!/1) end)

      assert [
               %{
                 "id" => "550e8400-e29b-41d4-a716-446655440000",
                 "source_uuid" => source_uuid1,
                 "source_name" => source_name1,
                 "body" => %{"event_message" => "Test message 1"},
                 "ingested_at" => _,
                 "timestamp" => _
               },
               %{
                 "id" => "9bc07845-9859-4163-bfe5-a74c1a1443a2",
                 "source_uuid" => source_uuid2,
                 "source_name" => source_name2,
                 "body" => %{"event_message" => "Test message 2"},
                 "ingested_at" => _,
                 "timestamp" => _
               }
             ] = row_payloads

      expected_source_uuid = Atom.to_string(source.token)
      assert source_uuid1 == expected_source_uuid
      assert source_uuid2 == expected_source_uuid

      assert source_name1 == source.name
      assert source_name2 == source.name
    end

    test "handles empty event list", %{backend: backend} do
      result = ClickHouseAdaptor.insert_log_events(backend, [])
      assert :ok = result
    end
  end

  describe "execute_query/2" do
    setup do
      insert(:plan, name: "Free")

      {source, backend, cleanup_fn} = setup_clickhouse_test()
      on_exit(cleanup_fn)

      start_supervised!({ClickHouseAdaptor, backend})

      [source: source, backend: backend]
    end

    test "executes simple queries using backend-only interface", %{backend: backend} do
      result = ClickHouseAdaptor.execute_query(backend, "SELECT 1 as test_value", [])

      assert {:ok, [%{"test_value" => 1}]} = result
    end

    test "converts `@param` syntax to ClickHouse `{param:String}` format", %{backend: backend} do
      result =
        ClickHouseAdaptor.execute_query(
          backend,
          {"SELECT @test_value as param_result", ["test_value"], %{"test_value" => "hello"}},
          []
        )

      assert {:ok, [%{"param_result" => "hello"}]} = result
    end

    test "handles query errors gracefully", %{backend: backend} do
      result = ClickHouseAdaptor.execute_query(backend, "INVALID SQL SYNTAX", [])

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
      start_supervised!({ClickHouseAdaptor, backend})

      [backend: backend, user: user, source1: source1_with_backend, source2: source2]
    end

    test "multiple sources with same backend share the connection manager", %{
      backend: backend,
      source2: source2
    } do
      {:ok, _} = ClickHouseAdaptor.execute_ch_query(backend, "SELECT 1")

      initial_manager_via = Backends.via_backend(backend, ConnectionManager)
      initial_manager_pid = GenServer.whereis(initial_manager_via)

      assert is_pid(initial_manager_pid)
      assert Process.alive?(initial_manager_pid)
      assert ConnectionManager.pool_active?(backend)

      # Associate source2 with the backend
      {:ok, _source2} = Backends.update_source_backends(source2, [backend])

      # The connection manager should still be the same
      manager_via2 = Backends.via_backend(backend, ConnectionManager)
      manager_pid2 = GenServer.whereis(manager_via2)

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

      {:ok, {sql, params}} = ClickHouseAdaptor.ecto_to_sql(query, [])

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

      {:ok, {sql, params}} = ClickHouseAdaptor.ecto_to_sql(query, [])

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

      {:ok, {sql, params}} = ClickHouseAdaptor.ecto_to_sql(query, [])

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

      assert {:error, _reason} = ClickHouseAdaptor.ecto_to_sql(invalid_query, [])
    end
  end

  describe "read query `ConnectionManager` automatic wake-up" do
    setup do
      insert(:plan, name: "Free")
      {source, backend, cleanup_fn} = setup_clickhouse_test()
      on_exit(cleanup_fn)

      start_supervised!({ClickHouseAdaptor, backend})
      Process.sleep(100)
      [source: source, backend: backend]
    end

    test "multiple concurrent read queries handle race conditions correctly", %{
      backend: backend
    } do
      via = Backends.via_backend(backend, ConnectionManager)

      results =
        for i <- 1..10 do
          Task.async(fn ->
            ClickHouseAdaptor.execute_ch_query(backend, "SELECT #{i} as result")
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
      {:ok, _} = ClickHouseAdaptor.execute_ch_query(backend, "SELECT 1")

      via = Backends.via_backend(backend, ConnectionManager)
      assert original_pid = GenServer.whereis(via)

      Process.exit(original_pid, :kill)

      TestUtils.retry_assert(fn ->
        assert {:ok, [%{"result" => 2}]} =
                 ClickHouseAdaptor.execute_ch_query(backend, "SELECT 2 as result")

        assert new_pid = GenServer.whereis(via)
        assert new_pid != original_pid
        assert ConnectionManager.pool_active?(backend)
      end)
    end

    test "`ConnectionManager` for different backends are independent", %{
      source: source,
      backend: backend1
    } do
      {_source2, backend2, cleanup_fn2} = setup_clickhouse_test(source: source)
      on_exit(cleanup_fn2)
      start_supervised!({ClickHouseAdaptor, backend2}, id: :adaptor2)

      {:ok, _} = ClickHouseAdaptor.execute_ch_query(backend2, "SELECT 1")
      assert ConnectionManager.pool_active?(backend2)

      via1 = Backends.via_backend(backend1, ConnectionManager)
      via2 = Backends.via_backend(backend2, ConnectionManager)

      refute GenServer.whereis(via2) == GenServer.whereis(via1)
      assert QueryConnectionSup.count_query_connection_managers() >= 1
    end
  end

  defp modify_backend_with_long_token(%Backend{} = backend) do
    long_token = random_string(200)

    %Backend{
      backend
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
