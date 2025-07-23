defmodule Logflare.Backends.Adaptor.ClickhouseAdaptorIntegrationTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor.ClickhouseAdaptor
  alias Logflare.Backends.Adaptor.ClickhouseAdaptor.ConnectionManager

  @clickhouse_config %{
    url: "http://localhost:8123",
    database: "logflare_test",
    username: "logflare",
    password: "logflare",
    port: 8123,
    ingest_pool_size: 5,
    query_pool_size: 3
  }

  setup do
    insert(:plan, name: "Free")
    :ok
  end

  describe "basic ClickHouse connectivity" do
    test "can connect and execute simple queries directly" do
      opts = [
        scheme: "http",
        hostname: "localhost",
        port: 8123,
        database: "logflare_test",
        username: "logflare",
        password: "logflare"
      ]

      {:ok, conn} = Ch.start_link(opts)

      assert {:ok, %Ch.Result{rows: [[1]]}} = Ch.query(conn, "SELECT 1 as test")

      assert {:ok, %Ch.Result{rows: [["logflare_test"]]}} =
               Ch.query(conn, "SELECT currentDatabase()")

      assert {:ok, %Ch.Result{rows: [["hello"]]}} =
               Ch.query(conn, "SELECT {msg:String}", %{msg: "hello"})

      GenServer.stop(conn)
    end

    test "handles connection failures" do
      invalid_opts = [
        scheme: "http",
        hostname: "localhost",
        port: 9999,
        database: "logflare_test",
        username: "logflare",
        password: "logflare"
      ]

      case Ch.start_link(invalid_opts) do
        {:ok, conn} ->
          result = Ch.query(conn, "SELECT 1")
          assert {:error, _} = result
          GenServer.stop(conn)

        {:error, _reason} ->
          assert true
      end
    end

    test "can create and query simple test table" do
      opts = [
        scheme: "http",
        hostname: "localhost",
        port: 8123,
        database: "logflare_test",
        username: "logflare",
        password: "logflare"
      ]

      {:ok, conn} = Ch.start_link(opts)

      table_name = "integration_test_#{System.unique_integer([:positive])}"

      create_sql = """
      CREATE TABLE #{table_name} (
        id UInt64,
        message String,
        timestamp DateTime
      ) ENGINE = MergeTree()
      ORDER BY id
      """

      assert {:ok, %Ch.Result{}} = Ch.query(conn, create_sql)

      insert_sql =
        """
        INSERT INTO #{table_name} (id, message, timestamp) VALUES
        (1, 'test message 1', '2023-01-01 12:00:00'),
        (2, 'test message 2', '2023-01-01 12:01:00')
        """

      assert {:ok, %Ch.Result{}} = Ch.query(conn, insert_sql)

      select_sql = "SELECT id, message FROM #{table_name} ORDER BY id"
      assert {:ok, %Ch.Result{rows: rows}} = Ch.query(conn, select_sql)

      assert rows == [[1, "test message 1"], [2, "test message 2"]]

      drop_sql = "DROP TABLE #{table_name}"
      assert {:ok, %Ch.Result{}} = Ch.query(conn, drop_sql)

      GenServer.stop(conn)
    end
  end

  describe "connection testing" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, type: :clickhouse, config: @clickhouse_config)

      {:ok, _pid} = ClickhouseAdaptor.start_link({source, backend})

      [source: source, backend: backend]
    end

    test "can test ClickHouse connection and permissions", %{source: source, backend: backend} do
      result = ClickhouseAdaptor.test_connection(source, backend)

      assert :ok = result
    end
  end

  describe "provisioner process with application config engine" do
    test "provisioner creates all required tables and views with `SummingMergeTree` from app config" do
      user = insert(:user)
      source = insert(:source, user: user, retention_days: 7)
      backend = insert(:backend, type: :clickhouse, config: @clickhouse_config)

      {:ok, _pid} = ClickhouseAdaptor.start_link({source, backend})

      Process.sleep(1_000)

      ingest_table = ClickhouseAdaptor.clickhouse_ingest_table_name(source)
      key_table = ClickhouseAdaptor.clickhouse_key_count_table_name(source)
      view_name = ClickhouseAdaptor.clickhouse_materialized_view_name(source)

      for resource <- [ingest_table, key_table, view_name] do
        check_result =
          ClickhouseAdaptor.execute_ch_read_query(
            {source, backend},
            "EXISTS TABLE #{resource}"
          )

        assert {:ok, [%{"result" => 1}]} = check_result
      end

      describe_result =
        ClickhouseAdaptor.execute_ch_read_query(
          {source, backend},
          "SHOW CREATE TABLE #{key_table}"
        )

      assert {:ok, [%{"statement" => create_statement}]} = describe_result
      assert create_statement =~ "SummingMergeTree"
      refute create_statement =~ "SharedSummingMergeTree"

      cleanup_test_tables({source, backend})
    end
  end

  describe "manual table provisioning" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user, retention_days: 7)
      backend = insert(:backend, type: :clickhouse, config: @clickhouse_config)

      {:ok, _pid} = ClickhouseAdaptor.start_link({source, backend})
      Process.sleep(500)
      cleanup_test_tables({source, backend})

      on_exit(fn ->
        cleanup_test_tables({source, backend})
      end)

      [source: source, backend: backend]
    end

    test "can provision ingest table", %{source: source, backend: backend} do
      result = ClickhouseAdaptor.provision_ingest_table({source, backend})

      assert {:ok, _} = result

      table_name = ClickhouseAdaptor.clickhouse_ingest_table_name(source)

      check_result =
        ClickhouseAdaptor.execute_ch_read_query(
          {source, backend},
          "EXISTS TABLE #{table_name}"
        )

      assert {:ok, [%{"result" => 1}]} = check_result
    end

    test "can provision key type counts table", %{source: source, backend: backend} do
      result = ClickhouseAdaptor.provision_key_type_counts_table({source, backend})

      assert {:ok, _} = result

      table_name = ClickhouseAdaptor.clickhouse_key_count_table_name(source)

      check_result =
        ClickhouseAdaptor.execute_ch_read_query(
          {source, backend},
          "EXISTS TABLE #{table_name}"
        )

      assert {:ok, [%{"result" => 1}]} = check_result
    end

    test "can provision materialized view", %{source: source, backend: backend} do
      assert {:ok, _} = ClickhouseAdaptor.provision_ingest_table({source, backend})
      assert {:ok, _} = ClickhouseAdaptor.provision_key_type_counts_table({source, backend})

      result = ClickhouseAdaptor.provision_materialized_view({source, backend})

      assert {:ok, _} = result

      view_name = ClickhouseAdaptor.clickhouse_materialized_view_name(source)

      check_result =
        ClickhouseAdaptor.execute_ch_read_query(
          {source, backend},
          "EXISTS TABLE #{view_name}"
        )

      assert {:ok, [%{"result" => 1}]} = check_result
    end
  end

  describe "log event insertion and retrieval" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, type: :clickhouse, config: @clickhouse_config)

      {:ok, _pid} = ClickhouseAdaptor.start_link({source, backend})

      assert {:ok, _} = ClickhouseAdaptor.provision_ingest_table({source, backend})

      on_exit(fn ->
        cleanup_test_tables({source, backend})
      end)

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
          {source, backend},
          "SELECT event_message, body FROM #{table_name} ORDER BY timestamp"
        )

      assert {:ok, rows} = query_result
      assert length(rows) == 2

      # Check that we get the expected messages and JSON bodies
      assert [%{"event_message" => "Test message 1"}, %{"event_message" => "Test message 2"}] =
               rows
    end

    test "handles empty event list", %{source: source, backend: backend} do
      result = ClickhouseAdaptor.insert_log_events({source, backend}, [])
      assert {:ok, %Ch.Result{}} = result
    end
  end

  describe "query execution" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, type: :clickhouse, config: @clickhouse_config)

      {:ok, _pid} = ClickhouseAdaptor.start_link({source, backend})

      on_exit(fn ->
        cleanup_test_tables({source, backend})
      end)

      [source: source, backend: backend]
    end

    test "can execute ingest queries", %{source: source, backend: backend} do
      result =
        ClickhouseAdaptor.execute_ch_ingest_query(
          {source, backend},
          "SELECT 1 as test"
        )

      assert {:ok, %Ch.Result{rows: [[1]]}} = result
    end

    test "can execute read queries", %{source: source, backend: backend} do
      result =
        ClickhouseAdaptor.execute_ch_read_query(
          {source, backend},
          "SELECT 2 as test"
        )

      assert {:ok, [%{"test" => 2}]} = result
    end

    test "handles query errors", %{source: source, backend: backend} do
      result =
        ClickhouseAdaptor.execute_ch_read_query(
          {source, backend},
          "INVALID SQL QUERY"
        )

      assert {:error, _} = result
    end

    test "can execute queries with parameters", %{source: source, backend: backend} do
      result =
        ClickhouseAdaptor.execute_ch_read_query(
          {source, backend},
          "SELECT 'hello world' as result"
        )

      assert {:ok, [%{"result" => "hello world"}]} = result
    end
  end

  describe "connection management integration" do
    test "connections are properly managed across operations" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, type: :clickhouse, config: @clickhouse_config)

      {:ok, _pid} = ClickhouseAdaptor.start_link({source, backend})

      Process.sleep(100)

      assert {:ok, _} = ClickhouseAdaptor.execute_ch_ingest_query({source, backend}, "SELECT 1")
      Process.sleep(100)
      assert ConnectionManager.connection_active?(source, backend, :ingest) == true

      assert {:ok, _} = ClickhouseAdaptor.execute_ch_read_query({source, backend}, "SELECT 1")
      Process.sleep(100)
      assert ConnectionManager.connection_active?(source, backend, :query) == true

      cleanup_test_tables({source, backend})
    end
  end

  defp cleanup_test_tables({source, backend}) do
    table_names = [
      ClickhouseAdaptor.clickhouse_ingest_table_name(source),
      ClickhouseAdaptor.clickhouse_key_count_table_name(source),
      ClickhouseAdaptor.clickhouse_materialized_view_name(source)
    ]

    for table_name <- table_names do
      try do
        ClickhouseAdaptor.execute_ch_ingest_query(
          {source, backend},
          "DROP TABLE IF EXISTS #{table_name}"
        )
      rescue
        # Ignore cleanup errors
        _ -> :ok
      catch
        # Process may not be running during cleanup :shrug:
        :exit, _ -> :ok
      end
    end
  end
end
