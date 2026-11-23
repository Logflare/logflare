defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.ProvisionerTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.Provisioner
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.QueryTemplates
  alias Logflare.Backends.Backend

  import Logflare.ClickHouseMappedEvents

  setup do
    insert(:plan, name: "Free")

    {source, backend} = setup_clickhouse_test()

    {:ok, supervisor_pid} = ClickHouseAdaptor.start_link(backend)

    on_exit(fn ->
      if Process.alive?(supervisor_pid) do
        Process.exit(supervisor_pid, :shutdown)
      end
    end)

    [source: source, backend: backend]
  end

  describe "child_spec/1" do
    test "returns correct child specification", %{backend: backend} do
      spec = Provisioner.child_spec(backend.id)

      assert spec.id == Provisioner
      assert spec.restart == :transient
      assert spec.start == {Provisioner, :start_link, [backend.id]}
    end
  end

  describe "successful provisioning flow" do
    test "a reachable backend", %{backend: backend} do
      pid = start_provisioned!(backend)

      assert Process.alive?(pid)
      assert_tables_exist(backend)
    end
  end

  describe "failed provisioning steps" do
    setup do
      Application.put_env(:logflare, Provisioner, initial_retry_ms: 10, max_retry_ms: 50)
      on_exit(fn -> Application.delete_env(:logflare, Provisioner) end)
    end

    @tag capture_log: true
    test "a step failing once" do
      grant_check_statement = QueryTemplates.grant_check_statement()

      for {failing_statement?, error} <- [
            {&(&1 == grant_check_statement),
             %DBConnection.ConnectionError{message: "unreachable"}},
            {&String.contains?(&1, "CREATE TABLE"),
             %Ch.Error{code: 497, message: "Not enough privileges"}}
          ] do
        {_source, backend} = setup_clickhouse_test()
        failures = :counters.new(1, [])

        stub(Ch, :query, fn pool, statement, params, opts ->
          if failing_statement?.(statement) and :counters.get(failures, 1) == 0 do
            :counters.add(failures, 1, 1)
            {:error, error}
          else
            Mimic.call_original(Ch, :query, [pool, statement, params, opts])
          end
        end)

        start_provisioner!(backend)

        TestUtils.retry_assert(fn -> assert_tables_exist(backend) end)
        assert 1 = :counters.get(failures, 1)
      end
    end

    @tag capture_log: true
    test "an ingest grant check failing on every attempt", %{backend: backend} do
      grant_check_statement = QueryTemplates.grant_check_statement()
      test_pid = self()

      stub(Ch, :query, fn
        _pool, ^grant_check_statement, _params, _opts ->
          send(test_pid, :grant_checked)
          {:error, %DBConnection.ConnectionError{message: "unreachable"}}

        pool, statement, params, opts ->
          Mimic.call_original(Ch, :query, [pool, statement, params, opts])
      end)

      pid = start_provisioner!(backend)

      for _attempt <- 1..3, do: assert_receive(:grant_checked, 1_000)
      assert Process.alive?(pid)
    end

    @tag capture_log: true
    test "a backend lookup failing once", %{backend: backend} do
      warm_backend_cache(backend)
      failures = :counters.new(1, [])

      stub(Backends, :get_backend, fn backend_id ->
        if :counters.get(failures, 1) == 0 do
          :counters.add(failures, 1, 1)
          raise DBConnection.ConnectionError, "connection not available"
        else
          Mimic.call_original(Backends, :get_backend, [backend_id])
        end
      end)

      start_provisioner!(backend)

      TestUtils.retry_assert(fn -> assert_tables_exist(backend) end)
      assert 1 = :counters.get(failures, 1)
    end

    @tag capture_log: true
    test "a database fixed between attempts", %{backend: backend} do
      warm_backend_cache(backend)
      backend = save_backend_config!(backend, %{database: "logflare_test_missing"})
      stub_missing_database("logflare_test_missing")

      start_provisioner!(backend)
      assert_receive {:connection_tested, "logflare_test_missing"}, 1_000

      backend = save_backend_config!(backend, %{database: "logflare_test"})

      TestUtils.retry_assert(fn -> assert_tables_exist(backend) end)
    end

    @tag capture_log: true
    test "a backend deleted between attempts", %{backend: backend} do
      stub_connection_test(fn _backend ->
        {:error, %DBConnection.ConnectionError{message: "unreachable"}}
      end)

      pid = start_provisioner!(backend)
      ref = Process.monitor(pid)
      assert_receive {:connection_tested, _database}, 1_000

      delete_backend!(backend)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000
    end
  end

  describe "config changes" do
    @tag capture_log: true
    test "a database change not yet in the cache", %{backend: backend} do
      pid = start_provisioned!(backend)
      stub_missing_database("logflare_test_missing")
      warm_backend_cache(backend)

      backend = save_backend_config!(backend, %{database: "logflare_test_missing"})
      :ok = ClickHouseAdaptor.on_backend_config_changed(backend)
      await_idle(pid)

      assert_received {:connection_tested, "logflare_test_missing"}
    end

    test "a provisioned backend whose batch size changed", %{backend: backend} do
      pid = start_provisioned!(backend)
      stub_connection_test(fn _backend -> :ok end)

      save_backend_config!(backend, %{batch_size: 2_000})
      :ok = Provisioner.config_changed(backend.id)
      await_idle(pid)

      assert Process.alive?(pid)
      refute_received {:connection_tested, _database}
    end

    @tag capture_log: true
    test "a backend fixed while waiting for a retry", %{backend: backend} do
      Application.put_env(:logflare, Provisioner, initial_retry_ms: :timer.minutes(1))
      on_exit(fn -> Application.delete_env(:logflare, Provisioner) end)

      backend = save_backend_config!(backend, %{database: "logflare_test_missing"})
      stub_missing_database("logflare_test_missing")

      pid = start_provisioner!(backend)
      assert_receive {:connection_tested, "logflare_test_missing"}, 1_000

      backend = save_backend_config!(backend, %{database: "logflare_test"})
      :ok = Provisioner.config_changed(backend.id)
      await_idle(pid)

      assert_tables_exist(backend)
    end
  end

  describe "connection test failure handling" do
    @tag capture_log: true
    test "provisions tables even when the read grant check fails" do
      {_source, read_backend} =
        setup_clickhouse_test(config: %{query_user: "ch_reader", query_password: "reader_pa55"})

      # A real authentication failure only surfaces after a multi-second pool queue
      # timeout, which leaves no headroom for the provisioning wait below.
      read_grant_statement = QueryTemplates.read_grant_check_statement()
      test_pid = self()

      stub(Ch, :query, fn
        _pool, ^read_grant_statement, _params, _opts ->
          send(test_pid, :read_grant_checked)
          {:error, %Ch.Error{code: 516, message: "ch_reader: Authentication failed"}}

        pool, statement, params, opts ->
          Mimic.call_original(Ch, :query, [pool, statement, params, opts])
      end)

      {:ok, adaptor_pid} = ClickHouseAdaptor.start_link(read_backend)

      on_exit(fn ->
        if Process.alive?(adaptor_pid) do
          Process.exit(adaptor_pid, :shutdown)
        end
      end)

      start_provisioned!(read_backend)
      assert_received :read_grant_checked

      {:ok, conn} =
        Ch.start_link(
          scheme: "http",
          hostname: "localhost",
          port: 8123,
          database: read_backend.config.database,
          username: read_backend.config.username,
          password: read_backend.config.password,
          pool_size: 1
        )

      for event_type <- [:log, :metric, :trace] do
        table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(read_backend, event_type)

        assert {:ok, %Ch.Result{rows: [[1]]}} = Ch.query(conn, "EXISTS TABLE #{table_name}")
      end

      GenServer.stop(conn)
    end
  end

  describe "provisioning idempotency" do
    test "can run multiple times without errors", %{backend: backend} do
      start_provisioned!(backend)
      stop_supervised!(backend.id)
      start_provisioned!(backend)

      for event_type <- [:log, :metric, :trace] do
        table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, event_type)

        assert {:ok, {[%{"count" => 0}], _bytes}} =
                 ClickHouseAdaptor.execute_ch_query(
                   backend,
                   "SELECT count(*) as count FROM #{table_name}"
                 )
      end
    end
  end

  describe "process lifecycle" do
    test "can insert data after provisioning completes", %{source: source, backend: backend} do
      start_provisioned!(backend)

      log_event = build_mapped_log_event(source: source, message: "Test after provisioning")
      :ok = ClickHouseAdaptor.insert_log_events(backend, [log_event], :log)

      Process.sleep(100)

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, :log)

      assert {:ok, {[%{"count" => 1}], _bytes}} =
               ClickHouseAdaptor.execute_ch_query(
                 backend,
                 "SELECT count(*) as count FROM #{table_name}"
               )
    end
  end

  describe "table schema verification" do
    test "creates logs table with correct OTEL schema", %{backend: backend} do
      start_provisioned!(backend)

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, :log)

      {:ok, {columns, _bytes}} =
        ClickHouseAdaptor.execute_ch_query(backend, "DESCRIBE TABLE #{table_name}")

      column_names = Enum.map(columns, & &1["name"])

      assert "id" in column_names
      assert "source_uuid" in column_names
      assert "source_name" in column_names
      assert "event_message" in column_names
      assert "log_attributes" in column_names
      assert "trace_id" in column_names
      assert "severity_text" in column_names
      assert "service_name" in column_names
      assert "timestamp" in column_names

      id_col = Enum.find(columns, &(&1["name"] == "id"))
      assert %{"type" => "UUID"} = id_col

      source_uuid_col = Enum.find(columns, &(&1["name"] == "source_uuid"))
      assert %{"type" => "LowCardinality(String)"} = source_uuid_col

      timestamp_col = Enum.find(columns, &(&1["name"] == "timestamp"))
      assert %{"type" => "DateTime64(9)"} = timestamp_col
    end

    test "creates metrics table with correct OTEL schema", %{backend: backend} do
      start_provisioned!(backend)

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, :metric)

      {:ok, {columns, _bytes}} =
        ClickHouseAdaptor.execute_ch_query(backend, "DESCRIBE TABLE #{table_name}")

      column_names = Enum.map(columns, & &1["name"])

      assert "id" in column_names
      assert "source_uuid" in column_names
      assert "source_name" in column_names
      assert "event_message" in column_names
      assert "time_unix" in column_names
      assert "start_time_unix" in column_names
      assert "metric_name" in column_names
      assert "metric_type" in column_names
      assert "attributes" in column_names
      assert "value" in column_names
      assert "bucket_counts" in column_names
      assert "timestamp" in column_names

      time_unix_col = Enum.find(columns, &(&1["name"] == "time_unix"))
      assert %{"type" => "Nullable(DateTime64(9))"} = time_unix_col

      timestamp_col = Enum.find(columns, &(&1["name"] == "timestamp"))
      assert %{"type" => "DateTime64(9)"} = timestamp_col
    end

    test "creates traces table with correct OTEL schema", %{backend: backend} do
      start_provisioned!(backend)

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, :trace)

      {:ok, {columns, _bytes}} =
        ClickHouseAdaptor.execute_ch_query(backend, "DESCRIBE TABLE #{table_name}")

      column_names = Enum.map(columns, & &1["name"])

      assert "id" in column_names
      assert "source_uuid" in column_names
      assert "source_name" in column_names
      assert "event_message" in column_names
      assert "trace_id" in column_names
      assert "span_id" in column_names
      assert "span_name" in column_names
      assert "span_kind" in column_names
      assert "duration" in column_names
      assert "span_attributes" in column_names
      assert "timestamp" in column_names

      timestamp_col = Enum.find(columns, &(&1["name"] == "timestamp"))
      assert %{"type" => "DateTime64(9)"} = timestamp_col
    end
  end

  defp start_provisioner!(backend) do
    start_supervised!(Supervisor.child_spec({Provisioner, backend.id}, id: backend.id))
  end

  defp start_provisioned!(backend) do
    pid = start_provisioner!(backend)
    await_idle(pid)
    pid
  end

  defp await_idle(pid) do
    _state = :sys.get_state(pid)
    :ok
  end

  defp stub_connection_test(result_fun) do
    test_pid = self()

    stub(ClickHouseAdaptor, :test_ingest_connection, fn backend ->
      send(test_pid, {:connection_tested, backend.config.database})
      result_fun.(backend)
    end)
  end

  defp stub_missing_database(database) do
    stub_connection_test(fn
      %Backend{config: %{database: ^database}} ->
        {:error, %Ch.Error{code: 81, message: "Database #{database} does not exist"}}

      backend ->
        Mimic.call_original(ClickHouseAdaptor, :test_ingest_connection, [backend])
    end)
  end

  defp warm_backend_cache(backend) do
    %Backend{} = Backends.Cache.get_backend(backend.id)
  end

  defp save_backend_config!(backend, changes) do
    backend
    |> Repo.reload!()
    |> Backend.changeset(%{config: Map.merge(backend.config, changes)})
    |> Repo.update!()

    Backends.get_backend(backend.id)
  end

  defp delete_backend!(backend) do
    Repo.delete_all(from(sb in "sources_backends", where: sb.backend_id == ^backend.id))
    Repo.delete!(backend)
  end

  defp assert_tables_exist(backend) do
    for event_type <- [:log, :metric, :trace] do
      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, event_type)

      assert {:ok, {[%{"result" => 1}], _bytes}} =
               ClickHouseAdaptor.execute_ch_query(backend, "EXISTS TABLE #{table_name}")
    end
  end
end
